{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

{- | Pull-based (iterator) execution engine.

Each operator returns a 'Stream' — an IO action that produces the next
'DataFrame' batch on each call and returns 'Nothing' when exhausted.
Blocking operators (Sort, HashJoin) materialise their input before producing
output.  HashAggregate uses streaming partial aggregation when all aggregate
expressions support it.
-}
module DataFrame.Lazy.Internal.Executor (
    ExecutorConfig (..),
    defaultExecutorConfig,
    execute,
    foldBatches,
) where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TBQueue (newTBQueueIO, readTBQueue, writeTBQueue)
import Control.DeepSeq (force)
import Control.Exception (evaluate)
import Control.Monad (filterM, when)
import qualified Data.ByteString as BS
import Data.IORef
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame.IO.Parquet as Parquet
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Expression as E
import DataFrame.Internal.Schema (elements)
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
import System.Directory (doesDirectoryExist)
import System.FilePath ((</>))
import System.FilePath.Glob (glob)
import System.IO (hClose)
import Type.Reflection (typeRep)

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
        , defaultBatchSize = 1_000_000
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

{- | Fold a function over every batch produced by a physical plan.
The fold is strict in the accumulator; each batch is discarded after folding.
-}
foldBatches ::
    (b -> D.DataFrame -> IO b) -> b -> PhysicalPlan -> ExecutorConfig -> IO b
foldBatches f seed plan cfg = do
    stream <- buildStream plan cfg
    let loop !acc = do
            mb <- pullBatch stream
            case mb of
                Nothing -> return acc
                Just batch -> do
                    !acc' <- f acc batch
                    loop acc'
    loop seed

-- ---------------------------------------------------------------------------
-- Per-operator stream builders
-- ---------------------------------------------------------------------------

buildStream :: PhysicalPlan -> ExecutorConfig -> IO Stream
-- Scan -----------------------------------------------------------------------
buildStream (PhysicalScan (CsvSource path sep) cfg) _ =
    executeCsvScan path sep cfg
buildStream (PhysicalScan (ParquetSource path) cfg) _ =
    executeParquetScan path cfg
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
-- HashAggregate --------------------------------------------------------------
buildStream (PhysicalHashAggregate keys aggs child) execCfg = do
    childStream <- buildStream child execCfg
    if all (isStreamableAgg . snd) aggs
        then do
            -- Streaming partial aggregation: O(|groups|) memory
            let (partialAggs, mergeAggs, finalizer) = buildAggPlan aggs
            accRef <- newIORef (Nothing :: Maybe D.DataFrame)
            let loop = do
                    mb <- pullBatch childStream
                    case mb of
                        Nothing -> return ()
                        Just batch -> do
                            -- Force to NF so the batch DataFrame can be GC'd immediately.
                            -- evaluate . force breaks the thunk chain that would otherwise
                            -- keep every batch (~60 MB each) alive until the end = OOM.
                            !partial <-
                                evaluate . force $ Agg.aggregate partialAggs (Agg.groupBy keys batch)
                            mAcc <- readIORef accRef
                            !newAcc <- case mAcc of
                                Nothing -> return partial
                                Just acc ->
                                    evaluate . force $
                                        Agg.aggregate mergeAggs $
                                            Agg.groupBy keys (acc <> partial)
                            writeIORef accRef (Just newAcc)
                            loop
            loop
            mFinal <- fmap (fmap finalizer) (readIORef accRef)
            ref <- newIORef mFinal
            return . Stream $ do
                mb <- readIORef ref
                writeIORef ref Nothing
                return mb
        else do
            -- Fallback: materialise entire child (for CollectAgg etc.)
            df <- collectStream childStream
            let result = Agg.aggregate aggs (Agg.groupBy keys df)
            ref <- newIORef (Just result)
            return . Stream $ do
                mb <- readIORef ref
                writeIORef ref Nothing
                return mb
-- SourceDF (split pre-loaded DataFrame into batches) -------------------------
buildStream (PhysicalSourceDF df) execCfg = do
    let bs = defaultBatchSize execCfg
        total = Core.nRows df
    posRef <- newIORef (0 :: Int)
    return . Stream $ do
        i <- readIORef posRef
        if i >= total
            then return Nothing
            else do
                let n = min bs (total - i)
                    batch = Sub.range (i, i + n) df
                writeIORef posRef (i + n)
                return (Just batch)
-- HashJoin — streaming probe (INNER/LEFT) or blocking fallback ----------------
buildStream (PhysicalHashJoin jt leftKey rightKey leftPlan rightPlan) execCfg =
    case jt of
        Join.INNER -> streamingHashJoin assembleInnerBatch
        Join.LEFT -> streamingHashJoin assembleLeftBatch
        _ -> do
            -- Blocking fallback for RIGHT / FULL_OUTER
            leftDf <- execute leftPlan execCfg
            rightDf <- execute rightPlan execCfg
            let result = performJoin jt leftKey rightKey leftDf rightDf
            ref <- newIORef (Just result)
            return . Stream $ do
                mb <- readIORef ref
                writeIORef ref Nothing
                return mb
  where
    streamingHashJoin assembleFn = do
        -- Materialise build (right) side once and build the compact index.
        rightDf <- execute rightPlan execCfg
        let rightDf' =
                if leftKey == rightKey
                    then rightDf
                    else Core.rename rightKey leftKey rightDf
            joinKey = leftKey
            csSet = S.fromList [joinKey]
            rightHashes = Join.buildHashColumn [joinKey] rightDf'
            ci = Join.buildCompactIndex rightHashes
        -- Stream probe (left) side batch by batch.
        leftStream <- buildStream leftPlan execCfg
        return . Stream $ do
            mBatch <- pullBatch leftStream
            case mBatch of
                Nothing -> return Nothing
                Just probeBatch -> do
                    let probeHashes = Join.buildHashColumn [joinKey] probeBatch
                        (probeIxs, buildIxs) = Join.hashProbeKernel ci probeHashes
                    return . Just $ assembleFn csSet probeBatch rightDf' probeIxs buildIxs

    assembleLeftBatch csSet probeBatch rightDf' probeIxs buildIxs =
        let batchN = Core.nRows probeBatch
            -- Mark which probe rows were matched (may have duplicates — that's fine).
            matched =
                VU.accumulate
                    (\_ b -> b)
                    (VU.replicate batchN False)
                    (VU.map (,True) probeIxs)
            unmatchedIxs = VU.findIndices not matched
            allProbeIxs = probeIxs VU.++ unmatchedIxs
            allBuildIxs = buildIxs VU.++ VU.replicate (VU.length unmatchedIxs) (-1)
         in Join.assembleLeft csSet probeBatch rightDf' allProbeIxs allBuildIxs

    assembleInnerBatch = Join.assembleInner

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
-- Streaming aggregation helpers
-- ---------------------------------------------------------------------------

{- | True when an aggregate expression can be computed incrementally
(i.e., partial results can be merged without materialising all rows).
-}
isStreamableAgg :: E.UExpr -> Bool
isStreamableAgg (E.UExpr (E.Agg (E.CollectAgg _ _) _)) = False
isStreamableAgg (E.UExpr (E.Agg (E.FoldAgg _ Nothing (_ :: a -> b -> a)) _)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> True -- self-merging: min, max, sum
        Nothing -> False
isStreamableAgg (E.UExpr (E.Agg (E.FoldAgg _ (Just _) (_ :: a -> b -> a)) _)) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl -> True -- seeded Int fold (old-style count): merge by sum
        Nothing ->
            case testEquality (typeRep @a) (typeRep @b) of
                Just Refl -> True -- seeded self-merging
                Nothing -> False
isStreamableAgg (E.UExpr (E.Agg (E.MergeAgg{}) _)) = True
isStreamableAgg _ = False

{- | Build the partial, merge, and finalizer plan for a list of streamable
aggregate expressions.

* @partialAggs@  — applied per batch, producing one row per group
* @mergeAggs@    — applied when combining two partial-result DataFrames
* @finalizer@    — post-process after all batches (needed for 'MergeAgg'
                   where the accumulator type differs from the output type)
-}
buildAggPlan ::
    [(T.Text, E.UExpr)] ->
    ( [(T.Text, E.UExpr)]
    , [(T.Text, E.UExpr)]
    , D.DataFrame -> D.DataFrame
    )
buildAggPlan aggs = foldl combine ([], [], id) (map processAgg aggs)
  where
    combine (p1, m1, f1) (p2, m2, f2) = (p1 ++ p2, m1 ++ m2, f1 . f2)

    processAgg ::
        (T.Text, E.UExpr) ->
        ([(T.Text, E.UExpr)], [(T.Text, E.UExpr)], D.DataFrame -> D.DataFrame)
    processAgg (name, ue) = case ue of
        -- Seedless FoldAgg: min, max, sum (self-merging when a = b)
        E.UExpr (E.Agg (E.FoldAgg n Nothing (f :: a -> b -> a)) (_ :: E.Expr b)) ->
            case testEquality (typeRep @a) (typeRep @b) of
                Just Refl ->
                    ( [(name, ue)]
                    , [(name, E.UExpr (E.Agg (E.FoldAgg n Nothing f) (E.Col @a name)))]
                    , id
                    )
                Nothing ->
                    -- a /= b but a = Int: merge by sum (backward compat)
                    case testEquality (typeRep @a) (typeRep @Int) of
                        Just Refl ->
                            ( [(name, ue)]
                            ,
                                [
                                    ( name
                                    , E.UExpr
                                        (E.Agg (E.FoldAgg "sum" Nothing ((+) :: Int -> Int -> Int)) (E.Col @Int name))
                                    )
                                ]
                            , id
                            )
                        Nothing -> ([(name, ue)], [(name, ue)], id)
        -- Seeded FoldAgg: old-style count (a = Int)
        E.UExpr (E.Agg (E.FoldAgg n (Just _) (f :: a -> b -> a)) (_ :: E.Expr b)) ->
            case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl ->
                    ( [(name, ue)]
                    ,
                        [
                            ( name
                            , E.UExpr
                                (E.Agg (E.FoldAgg "sum" Nothing ((+) :: Int -> Int -> Int)) (E.Col @Int name))
                            )
                        ]
                    , id
                    )
                Nothing ->
                    case testEquality (typeRep @a) (typeRep @b) of
                        Just Refl ->
                            ( [(name, ue)]
                            , [(name, E.UExpr (E.Agg (E.FoldAgg n Nothing f) (E.Col @a name)))]
                            , id
                            )
                        Nothing -> ([(name, ue)], [(name, ue)], id)
        -- MergeAgg: count, mean, etc.
        -- Partial step: accumulate into acc type (using id as finalizer).
        -- Merge step: apply merge function to two acc-typed partial results.
        -- Finalizer: apply fin to convert acc column to output type.
        E.UExpr
            ( E.Agg
                    ( E.MergeAgg
                            n
                            seed
                            (step :: acc -> b -> acc)
                            (merge :: acc -> acc -> acc)
                            (fin :: acc -> a)
                        )
                    (inner :: E.Expr b)
                ) ->
                let partialExpr =
                        E.UExpr
                            ( E.Agg
                                (E.MergeAgg n seed step merge (id :: acc -> acc))
                                inner
                            )
                    mergeExpr =
                        E.UExpr
                            ( E.Agg
                                (E.FoldAgg ("merge_" <> n) Nothing merge)
                                (E.Col @acc name)
                            )
                    finalize df =
                        let accCol = D.unsafeGetColumn name df
                            finalCol =
                                either
                                    (error "buildAggPlan: MergeAgg finalize failed")
                                    id
                                    (C.mapColumn @acc @a fin accCol)
                         in Core.insertColumn name finalCol df
                 in ( [(name, partialExpr)]
                    , [(name, mergeExpr)]
                    , finalize
                    )
        _ -> ([(name, ue)], [(name, ue)], id)

-- ---------------------------------------------------------------------------
-- Parquet scan implementation
-- ---------------------------------------------------------------------------

{- | Scan a Parquet file, directory, or glob.  Each file becomes one batch.
Column projection and predicate pushdown are forwarded to 'readParquetWithOpts'
via 'ParquetReadOptions'.
-}
executeParquetScan :: FilePath -> ScanConfig -> IO Stream
executeParquetScan path cfg
    | Parquet.isHFUri path = executeHFParquetScan path cfg
    | otherwise = do
        isDir <- doesDirectoryExist path
        let pat = if isDir then path </> "*" else path
        matches <- glob pat
        files <- filterM (fmap not . doesDirectoryExist) matches
        when (null files) $
            error ("executeParquetScan: no parquet files found for " ++ path)
        let opts =
                Parquet.defaultParquetReadOptions
                    { Parquet.selectedColumns = Just (M.keys (elements (scanSchema cfg)))
                    , Parquet.predicate = scanPushdownPredicate cfg
                    }
        ref <- newIORef files
        return . Stream $ do
            fs <- readIORef ref
            case fs of
                [] -> return Nothing
                (f : rest) -> do
                    writeIORef ref rest
                    Just <$> Parquet.readParquetWithOpts opts f

{- | HuggingFace Parquet scan.  Files are resolved once (API call or direct URL)
then downloaded one at a time as the stream is pulled — so only one file's worth
of data is in memory at a time, regardless of dataset size.
-}

-- TODO: mchavinda - this should be a more general online file scanner.
executeHFParquetScan :: FilePath -> ScanConfig -> IO Stream
executeHFParquetScan path cfg = do
    ref <- case Parquet.parseHFUri path of
        Left err -> error err
        Right r -> pure r
    mToken <- Parquet.getHFToken
    hfFiles <-
        if Parquet.hasGlob (Parquet.hfGlob ref)
            then Parquet.resolveHFUrls mToken ref
            else do
                let url = Parquet.directHFUrl ref
                    filename = last $ T.splitOn "/" (Parquet.hfGlob ref)
                pure [Parquet.HFParquetFile url "" "" filename]
    when (null hfFiles) $
        error ("executeParquetScan: no HF parquet files found for " ++ path)
    let opts =
            Parquet.defaultParquetReadOptions
                { Parquet.selectedColumns = Just (M.keys (elements (scanSchema cfg)))
                , Parquet.predicate = scanPushdownPredicate cfg
                }
    filesRef <- newIORef hfFiles
    return . Stream $ do
        fs <- readIORef filesRef
        case fs of
            [] -> return Nothing
            (f : rest) -> do
                writeIORef filesRef rest
                -- Download a single file, read it, then return the batch.
                [localPath] <- Parquet.downloadHFFiles mToken [f]
                Just <$> Parquet.readParquetWithOpts opts localPath

-- ---------------------------------------------------------------------------
-- CSV scan implementation
-- ---------------------------------------------------------------------------

{- | CSV scan with pipeline parallelism: a dedicated reader thread fills a
bounded queue while the caller's thread applies pushdown predicates and
delivers batches to the rest of the pipeline.  The queue depth of 8 keeps
at most eight raw batches in flight, bounding memory while hiding I/O latency.
-}
executeCsvScan :: FilePath -> Char -> ScanConfig -> IO Stream
executeCsvScan path sep cfg = do
    (handle, colSpec) <- LCSV.openCsvStream sep (scanSchema cfg) path
    -- Queue carries raw batches; Nothing is the end-of-stream sentinel.
    -- Depth 2: each batch holds ~60 MB (1M Text + Double columns); 8 would be ~480 MB.
    queue <- newTBQueueIO 2
    _ <- forkIO $ do
        let loop lo = do
                result <- LCSV.readBatch sep colSpec (scanBatchSize cfg) lo handle
                case result of
                    Nothing ->
                        hClose handle >> atomically (writeTBQueue queue Nothing)
                    Just (df, lo') ->
                        atomically (writeTBQueue queue (Just df)) >> loop lo'
        loop BS.empty
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
