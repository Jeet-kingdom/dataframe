{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Lazy.IO.CSV where

import qualified Data.Map as M
import qualified Data.Proxy as P
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (forM_, unless, when, zipWithM_)
import Data.Attoparsec.Text (IResult (..), parseWith)
import Data.Char (intToDigit)
import Data.IORef
import Data.Maybe (fromMaybe, isJust)
import Data.Type.Equality (TestEquality (testEquality))
import DataFrame.Internal.Column (
    Column (..),
    MutableColumn (..),
    columnLength,
    freezeColumn',
    writeColumn,
 )
import DataFrame.Internal.DataFrame (DataFrame (..))
import DataFrame.Internal.Parsing
import DataFrame.Internal.Schema (Schema, SchemaType (..), elements)
import System.IO
import Type.Reflection
import Prelude hiding (takeWhile)

-- | Record for CSV read options.
data ReadOptions = ReadOptions
    { hasHeader :: Bool
    , inferTypes :: Bool
    , safeRead :: Bool
    , rowRange :: !(Maybe (Int, Int)) -- (start, length)
    , seekPos :: !(Maybe Integer)
    , totalRows :: !(Maybe Int)
    , leftOver :: !T.Text
    , rowsRead :: !Int
    }

{- | By default we assume the file has a header, we infer the types on read
and we convert any rows with nullish objects into Maybe (safeRead).
-}
defaultOptions :: ReadOptions
defaultOptions =
    ReadOptions
        { hasHeader = True
        , inferTypes = True
        , safeRead = True
        , rowRange = Nothing
        , seekPos = Nothing
        , totalRows = Nothing
        , leftOver = ""
        , rowsRead = 0
        }

{- | Reads a CSV file from the given path.
Note this file stores intermediate temporary files
while converting the CSV from a row to a columnar format.
-}
readCsv :: FilePath -> IO DataFrame
readCsv path = fst <$> readSeparated ',' defaultOptions path

{- | Reads a tab separated file from the given path.
Note this file stores intermediate temporary files
while converting the CSV from a row to a columnar format.
-}
readTsv :: FilePath -> IO DataFrame
readTsv path = fst <$> readSeparated '\t' defaultOptions path

-- | Reads a character separated file into a dataframe using mutable vectors.
readSeparated ::
    Char -> ReadOptions -> FilePath -> IO (DataFrame, (Integer, T.Text, Int))
readSeparated c opts path = do
    totalRows <- case totalRows opts of
        Nothing ->
            countRows c path >>= \total -> if hasHeader opts then return (total - 1) else return total
        Just n -> if hasHeader opts then return (n - 1) else return n
    let (_, len) = case rowRange opts of
            Nothing -> (0, totalRows)
            Just (start, len) -> (start, min len (totalRows - rowsRead opts))
    withFile path ReadMode $ \handle -> do
        firstRow <- fmap T.strip . parseSep c <$> TIO.hGetLine handle
        let columnNames =
                if hasHeader opts
                    then fmap (T.filter (/= '\"')) firstRow
                    else fmap (T.singleton . intToDigit) [0 .. (length firstRow - 1)]
        -- If there was no header rewind the file cursor.
        unless (hasHeader opts) $ hSeek handle AbsoluteSeek 0

        currPos <- hTell handle
        when (isJust $ seekPos opts) $
            hSeek handle AbsoluteSeek (fromMaybe currPos (seekPos opts))

        -- Initialize mutable vectors for each column
        let numColumns = length columnNames
        let numRows = len
        -- Use this row to infer the types of the rest of the column.
        (dataRow, remainder) <- readSingleLine c (leftOver opts) handle

        -- This array will track the indices of all null values for each column.
        nullIndices <- VM.unsafeNew numColumns
        VM.set nullIndices []
        mutableCols <- VM.unsafeNew numColumns
        getInitialDataVectors numRows mutableCols dataRow

        -- Read rows into the mutable vectors
        (unconsumed, r) <-
            fillColumns numRows c mutableCols nullIndices remainder handle

        -- Freeze the mutable vectors into immutable ones
        nulls' <- V.unsafeFreeze nullIndices
        cols <- V.mapM (freezeColumn mutableCols nulls' opts) (V.generate numColumns id)
        pos <- hTell handle

        return
            ( DataFrame
                { columns = cols
                , columnIndices = M.fromList (zip columnNames [0 ..])
                , dataframeDimensions = (maybe 0 columnLength (cols V.!? 0), V.length cols)
                , derivingExpressions = M.empty
                }
            , (pos, unconsumed, r + 1)
            )
{-# INLINE readSeparated #-}

getInitialDataVectors :: Int -> VM.IOVector MutableColumn -> [T.Text] -> IO ()
getInitialDataVectors n mCol xs = do
    forM_ (zip [0 ..] xs) $ \(i, x) -> do
        col <- case inferValueType x of
            "Int" ->
                MUnboxedColumn
                    <$> ( (VUM.unsafeNew n :: IO (VUM.IOVector Int)) >>= \c -> VUM.unsafeWrite c 0 (fromMaybe 0 $ readInt x) >> return c
                        )
            "Double" ->
                MUnboxedColumn
                    <$> ( (VUM.unsafeNew n :: IO (VUM.IOVector Double)) >>= \c -> VUM.unsafeWrite c 0 (fromMaybe 0 $ readDouble x) >> return c
                        )
            _ ->
                MBoxedColumn
                    <$> ( (VM.unsafeNew n :: IO (VM.IOVector T.Text)) >>= \c -> VM.unsafeWrite c 0 x >> return c
                        )
        VM.unsafeWrite mCol i col
{-# INLINE getInitialDataVectors #-}

-- | Reads rows from the handle and stores values in mutable vectors.
fillColumns ::
    Int ->
    Char ->
    VM.IOVector MutableColumn ->
    VM.IOVector [(Int, T.Text)] ->
    T.Text ->
    Handle ->
    IO (T.Text, Int)
fillColumns n c mutableCols nullIndices unused handle = do
    input <- newIORef unused
    rowsRead <- newIORef (0 :: Int)
    forM_ [1 .. (n - 1)] $ \i -> do
        isEOF <- hIsEOF handle
        input' <- readIORef input
        unless (isEOF && input' == mempty) $ do
            parseWith (TIO.hGetChunk handle) (parseRow c) input' >>= \case
                Fail unconsumed ctx er -> do
                    erpos <- hTell handle
                    fail $
                        "Failed to parse CSV file around "
                            <> show erpos
                            <> " byte; due: "
                            <> show er
                            <> "; context: "
                            <> show ctx
                Partial _ -> do
                    fail "Partial handler is called"
                Done (unconsumed :: T.Text) (row :: [T.Text]) -> do
                    writeIORef input unconsumed
                    modifyIORef rowsRead (+ 1)
                    zipWithM_ (writeValue mutableCols nullIndices i) [0 ..] row
    l <- readIORef input
    r <- readIORef rowsRead
    pure (l, r)
{-# INLINE fillColumns #-}

-- | Writes a value into the appropriate column, resizing the vector if necessary.
writeValue ::
    VM.IOVector MutableColumn ->
    VM.IOVector [(Int, T.Text)] ->
    Int ->
    Int ->
    T.Text ->
    IO ()
writeValue mutableCols nullIndices count colIndex value = do
    col <- VM.unsafeRead mutableCols colIndex
    res <- writeColumn count value col
    let modify value = VM.unsafeModify nullIndices ((count, value) :) colIndex
    either modify (const (return ())) res
{-# INLINE writeValue #-}

-- | Freezes a mutable vector into an immutable one, trimming it to the actual row count.
freezeColumn ::
    VM.IOVector MutableColumn ->
    V.Vector [(Int, T.Text)] ->
    ReadOptions ->
    Int ->
    IO Column
freezeColumn mutableCols nulls opts colIndex = do
    col <- VM.unsafeRead mutableCols colIndex
    freezeColumn' (nulls V.! colIndex) col
{-# INLINE freezeColumn #-}

-- ---------------------------------------------------------------------------
-- Streaming scan API
-- ---------------------------------------------------------------------------

{- | Open a CSV/separated file for streaming, returning an open handle
(positioned just after the header line) and the column specification
for the schema columns that appear in the file header.

The caller is responsible for closing the handle when done.
-}
openCsvStream ::
    Char ->
    Schema ->
    FilePath ->
    IO (Handle, [(Int, T.Text, SchemaType)])
openCsvStream sep schema path = do
    handle <- openFile path ReadMode
    headerLine <- TIO.hGetLine handle
    let headerCols = fmap (T.filter (/= '"') . T.strip) (parseSep sep headerLine)
    let schemaMap = elements schema
    let colSpec =
            [ (idx, name, stype)
            | (idx, name) <- zip [0 ..] headerCols
            , Just stype <- [M.lookup name schemaMap]
            ]
    when (null colSpec) $
        hClose handle
            >> fail
                ("openCsvStream: none of the schema columns appear in the header of " <> path)
    return (handle, colSpec)

{- | Read up to @batchSz@ rows from the open handle, returning a batch
'DataFrame' and the unconsumed leftover text.  Returns 'Nothing' when
the handle is at EOF and there is no leftover input.

The caller must pass the leftover returned by the previous call (use @""@
for the first call).
-}
readBatch ::
    Char ->
    [(Int, T.Text, SchemaType)] ->
    Int ->
    T.Text ->
    Handle ->
    IO (Maybe (DataFrame, T.Text))
readBatch sep colSpec batchSz leftover handle = do
    isEof <- hIsEOF handle
    if isEof && T.null leftover
        then return Nothing
        else do
            let numCols = length colSpec
            nullsArr <- VM.unsafeNew numCols
            VM.set nullsArr []
            mCols <- VM.unsafeNew numCols
            forM_ (zip [0 ..] colSpec) $ \(ci, (_, _, st)) ->
                VM.unsafeWrite mCols ci =<< makeCol batchSz st
            leftoverRef <- newIORef leftover
            rowCountRef <- newIORef (0 :: Int)
            let loop = do
                    rc <- readIORef rowCountRef
                    when (rc < batchSz) $ do
                        lo <- readIORef leftoverRef
                        eof <- hIsEOF handle
                        unless (eof && T.null lo) $ do
                            parseWith (TIO.hGetChunk handle) (parseRow sep) lo >>= \case
                                Fail _ ctx er ->
                                    fail
                                        ( "readBatch: parse error near row "
                                            <> ( show rc
                                                    <> ( ": "
                                                            <> ( er
                                                                    <> ( " ctx: "
                                                                            ++ show ctx
                                                                       )
                                                               )
                                                       )
                                               )
                                        )
                                Partial _ -> fail "readBatch: unexpected Partial"
                                Done rest row -> do
                                    writeIORef leftoverRef rest
                                    forM_ (zip [0 ..] colSpec) $ \(ci, (fi, _, _)) -> do
                                        let val = if fi < length row then row !! fi else ""
                                        col <- VM.unsafeRead mCols ci
                                        res <- writeColumn rc val col
                                        case res of
                                            Left nv -> VM.unsafeModify nullsArr ((rc, nv) :) ci
                                            Right _ -> return ()
                                    modifyIORef' rowCountRef (+ 1)
                                    loop
            loop
            actualRows <- readIORef rowCountRef
            finalLeftover <- readIORef leftoverRef
            if actualRows == 0
                then return Nothing
                else do
                    forM_ [0 .. numCols - 1] $ \ci -> do
                        col <- VM.unsafeRead mCols ci
                        VM.unsafeWrite mCols ci (sliceCol actualRows col)
                    nullsVec <- V.unsafeFreeze nullsArr
                    cols <- V.generateM numCols $ \ci -> do
                        col <- VM.unsafeRead mCols ci
                        freezeColumn' (nullsVec V.! ci) col
                    let colNames = [name | (_, name, _) <- colSpec]
                    return $
                        Just
                            ( DataFrame
                                { columns = cols
                                , columnIndices = M.fromList (zip colNames [0 ..])
                                , dataframeDimensions = (actualRows, numCols)
                                , derivingExpressions = M.empty
                                }
                            , finalLeftover
                            )

-- | Allocate a fresh 'MutableColumn' for @n@ slots based on a 'SchemaType'.
makeCol :: Int -> SchemaType -> IO MutableColumn
makeCol n (SType (_ :: P.Proxy a)) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl -> MUnboxedColumn <$> (VUM.unsafeNew n :: IO (VUM.IOVector Int))
        Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
            Just Refl -> MUnboxedColumn <$> (VUM.unsafeNew n :: IO (VUM.IOVector Double))
            Nothing -> MBoxedColumn <$> (VM.unsafeNew n :: IO (VM.IOVector T.Text))

-- | Slice a 'MutableColumn' to @n@ elements (no-copy view).
sliceCol :: Int -> MutableColumn -> MutableColumn
sliceCol n (MBoxedColumn col) = MBoxedColumn (VM.take n col)
sliceCol n (MUnboxedColumn col) = MUnboxedColumn (VUM.take n col)
