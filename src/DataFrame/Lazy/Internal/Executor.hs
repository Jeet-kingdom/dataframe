{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Pull-based (iterator) execution engine.

Each operator returns a 'Stream' — an IO action that produces the next
'DataFrame' batch on each call and returns 'Nothing' when exhausted.
Blocking operators (Sort, HashAggregate, HashJoin) materialise their input
before producing output.
-}
module DataFrame.Lazy.Internal.Executor (
    ExecutorConfig (..),
    defaultExecutorConfig,
    execute,
) where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TBQueue (newTBQueueIO, readTBQueue, writeTBQueue)
import Data.IORef
import qualified Data.Text as T
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Expression as E
import qualified DataFrame.Lazy.IO.Binary as Bin
import qualified DataFrame.Lazy.IO.CSV as LCSV
import DataFrame.Lazy.Internal.LogicalPlan (DataSource (..), SortOrder (..))
import DataFrame.Lazy.Internal.PhysicalPlan
import qualified DataFrame.Operations.Aggregation as Agg
import qualified DataFrame.Operations.Core as Core
import qualified DataFrame.Operations.Join as Join
import DataFrame.Operations.Merge ()
import qualified DataFrame.Operations.Permutation as Perm
import qualified DataFrame.Operations.Subset as Sub
import qualified DataFrame.Operations.Transformations as Trans
import System.IO (hClose)

-- ---------------------------------------------------------------------------
-- Configuration
-- ---------------------------------------------------------------------------

data ExecutorConfig = ExecutorConfig
    { memoryBudgetBytes :: !Int
    -- ^ Per-node spill threshold (currently informational; not enforced yet).
    , spillDirectory :: FilePath
    , defaultBatchSize :: !Int
    }

defaultExecutorConfig :: ExecutorConfig
defaultExecutorConfig =
    ExecutorConfig
        { memoryBudgetBytes = 512 * 1_048_576 -- 512 MiB
        , spillDirectory = "/tmp"
        , defaultBatchSize = 512_000
        }

-- ---------------------------------------------------------------------------
-- Stream abstraction
-- ---------------------------------------------------------------------------

{- | A pull-based stream: each call to the action yields the next batch or
'Nothing' when the stream is exhausted.  State is captured by the closure.
-}
newtype Stream = Stream {pullBatch :: IO (Maybe D.DataFrame)}

-- | Drain all batches from a stream and concatenate them into one DataFrame.
collectStream :: Stream -> IO D.DataFrame
collectStream stream = go D.empty
  where
    go acc = do
        mb <- pullBatch stream
        case mb of
            Nothing -> return acc
            Just df -> go (acc <> df)

-- ---------------------------------------------------------------------------
-- Top-level entry point
-- ---------------------------------------------------------------------------

{- | Execute a physical plan, returning the complete result as a single
'DataFrame'.
-}
execute :: PhysicalPlan -> ExecutorConfig -> IO D.DataFrame
execute plan cfg = buildStream plan cfg >>= collectStream

-- ---------------------------------------------------------------------------
-- Per-operator stream builders
-- ---------------------------------------------------------------------------

buildStream :: PhysicalPlan -> ExecutorConfig -> IO Stream
-- Scan -----------------------------------------------------------------------
buildStream (PhysicalScan (CsvSource path sep) cfg) _ =
    executeCsvScan path sep cfg
buildStream (PhysicalScan (ParquetSource _path) _cfg) _ =
    fail "Executor: Parquet source not yet supported in the lazy executor"
buildStream (PhysicalSpill child path) execCfg = do
    df <- execute child execCfg
    Bin.spillToDisk path df
    df' <- Bin.readSpilled path
    ref <- newIORef (Just df')
    return . Stream $
        ( do
            mb <- readIORef ref
            writeIORef ref Nothing
            return mb
        )
-- Filter ---------------------------------------------------------------------
buildStream (PhysicalFilter p child) execCfg = do
    childStream <- buildStream child execCfg
    return . Stream $
        ( do
            mb <- pullBatch childStream
            return $ fmap (Sub.filterWhere p) mb
        )
-- Project --------------------------------------------------------------------
buildStream (PhysicalProject cols child) execCfg = do
    childStream <- buildStream child execCfg
    return . Stream $
        ( do
            mb <- pullBatch childStream
            return $ fmap (Sub.select cols) mb
        )
-- Derive ---------------------------------------------------------------------
buildStream (PhysicalDerive name uexpr child) execCfg = do
    childStream <- buildStream child execCfg
    return . Stream $
        ( do
            mb <- pullBatch childStream
            return $ fmap (Trans.deriveMany [(name, uexpr)]) mb
        )
-- Limit ----------------------------------------------------------------------
buildStream (PhysicalLimit n child) execCfg = do
    childStream <- buildStream child execCfg
    countRef <- newIORef (0 :: Int)
    return . Stream $
        ( do
            remaining <- readIORef countRef
            if remaining >= n
                then return Nothing
                else do
                    mb <- pullBatch childStream
                    case mb of
                        Nothing -> return Nothing
                        Just df -> do
                            let toTake = min (Core.nRows df) (n - remaining)
                            modifyIORef' countRef (+ toTake)
                            return $ Just (Sub.take toTake df)
        )
-- Sort (blocking) ------------------------------------------------------------
buildStream (PhysicalSort cols child) execCfg = do
    df <- execute child execCfg
    let sortOrds = fmap toPermSortOrder cols
    let sorted = Perm.sortBy sortOrds df
    ref <- newIORef (Just sorted)
    return . Stream $
        ( do
            mb <- readIORef ref
            writeIORef ref Nothing
            return mb
        )
-- HashAggregate (blocking) ---------------------------------------------------
buildStream (PhysicalHashAggregate keys aggs child) execCfg = do
    df <- execute child execCfg
    let grouped = Agg.groupBy keys df
    let result = Agg.aggregate aggs grouped
    ref <- newIORef (Just result)
    return . Stream $
        ( do
            mb <- readIORef ref
            writeIORef ref Nothing
            return mb
        )
-- HashJoin (blocking on both sides) ------------------------------------------
buildStream (PhysicalHashJoin jt leftKey rightKey leftPlan rightPlan) execCfg = do
    leftDf <- execute leftPlan execCfg
    rightDf <- execute rightPlan execCfg
    let result = performJoin jt leftKey rightKey leftDf rightDf
    ref <- newIORef (Just result)
    return . Stream $
        ( do
            mb <- readIORef ref
            writeIORef ref Nothing
            return mb
        )
-- SortMergeJoin (blocking on both sides) -------------------------------------
buildStream (PhysicalSortMergeJoin jt leftKey rightKey leftPlan rightPlan) execCfg = do
    leftDf <- execute leftPlan execCfg
    rightDf <- execute rightPlan execCfg
    let result = performJoin jt leftKey rightKey leftDf rightDf
    ref <- newIORef (Just result)
    return . Stream $
        ( do
            mb <- readIORef ref
            writeIORef ref Nothing
            return mb
        )

-- ---------------------------------------------------------------------------
-- CSV scan implementation
-- ---------------------------------------------------------------------------

{- | CSV scan with pipeline parallelism: a dedicated reader thread fills a
bounded queue while the caller's thread applies pushdown predicates and
delivers batches to the rest of the pipeline.  The queue depth of 2 keeps
at most two raw batches in flight, bounding memory while hiding I/O latency.
-}
executeCsvScan :: FilePath -> Char -> ScanConfig -> IO Stream
executeCsvScan path sep cfg = do
    (handle, colSpec) <- LCSV.openCsvStream sep (scanSchema cfg) path
    -- Queue carries raw batches; Nothing is the end-of-stream sentinel.
    queue <- newTBQueueIO 2
    _ <- forkIO $ do
        let loop lo = do
                result <- LCSV.readBatch sep colSpec (scanBatchSize cfg) lo handle
                case result of
                    Nothing ->
                        hClose handle >> atomically (writeTBQueue queue Nothing)
                    Just (df, lo') ->
                        atomically (writeTBQueue queue (Just df)) >> loop lo'
        loop ""
    return . Stream $
        ( do
            mb <- atomically (readTBQueue queue)
            case mb of
                -- Re-insert the sentinel so repeated pulls after EOF stay Nothing.
                Nothing -> atomically (writeTBQueue queue Nothing) >> return Nothing
                Just df ->
                    let df' = case scanPushdownPredicate cfg of
                            Nothing -> df
                            Just p -> Sub.filterWhere p df
                     in return (Just df')
        )

-- ---------------------------------------------------------------------------
-- Join helper
-- ---------------------------------------------------------------------------

{- | Route join to the existing Operations.Join implementation.
When the left and right key names differ, rename the right key before joining.
-}
performJoin ::
    Join.JoinType -> T.Text -> T.Text -> D.DataFrame -> D.DataFrame -> D.DataFrame
performJoin jt leftKey rightKey leftDf rightDf =
    if leftKey == rightKey
        then Join.join jt [leftKey] rightDf leftDf
        else
            let rightRenamed = Core.rename rightKey leftKey rightDf
             in Join.join jt [leftKey] rightRenamed leftDf

-- ---------------------------------------------------------------------------
-- Sort order conversion
-- ---------------------------------------------------------------------------

-- | Convert plan-level sort order to the Permutation module's SortOrder.
toPermSortOrder :: (T.Text, SortOrder) -> Perm.SortOrder
toPermSortOrder (col, Ascending) = Perm.Asc (E.Col @T.Text col)
toPermSortOrder (col, Descending) = Perm.Desc (E.Col @T.Text col)
