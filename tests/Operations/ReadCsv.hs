{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- Test fixtures inspired by csv-spectrum (https://github.com/max-mapper/csv-spectrum)

module Operations.ReadCsv where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D

import Data.Function (on)
import Data.Maybe (fromMaybe)
import Data.Type.Equality (testEquality, (:~:) (Refl))
import DataFrame.Internal.Column (Column (..), bitmapTestBit, columnTypeString)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    columnIndices,
    columns,
    dataframeDimensions,
    getColumn,
 )
import System.Directory (removeFile)
import System.IO (IOMode (..), withFile)
import Test.HUnit
import Type.Reflection (typeRep)

fixtureDir :: FilePath
fixtureDir = "./tests/data/unstable_csv/"

tempDir :: FilePath
tempDir = "./tests/data/unstable_csv/"

readCsvNoInfer :: FilePath -> IO DataFrame
readCsvNoInfer =
    D.readCsvWithOpts
        D.defaultReadOptions{D.typeSpec = D.NoInference}

--------------------------------------------------------------------------------
-- Pretty-printer
--------------------------------------------------------------------------------

prettyPrintCsv :: FilePath -> DataFrame -> IO ()
prettyPrintCsv = prettyPrintSeparated ','

prettyPrintTsv :: FilePath -> DataFrame -> IO ()
prettyPrintTsv = prettyPrintSeparated '\t'

prettyPrintSeparated :: Char -> FilePath -> DataFrame -> IO ()
prettyPrintSeparated sep filepath df = withFile filepath WriteMode $ \handle -> do
    let (rows, _) = dataframeDimensions df
    let headers = map fst (L.sortBy (compare `on` snd) (M.toList (columnIndices df)))
    TIO.hPutStrLn
        handle
        (T.intercalate (T.singleton sep) (map (escapeField sep) headers))
    -- Write data rows
    mapM_
        (TIO.hPutStrLn handle . T.intercalate (T.singleton sep) . getRowEscaped sep df)
        [0 .. rows - 1]

-- Note: The unstable parser does not unescape doubled quotes (""  -> "),
-- so we must not double-escape them here. We only wrap in quotes when needed.
escapeField :: Char -> T.Text -> T.Text
escapeField sep field
    | needsQuoting = T.concat ["\"", field, "\""]
    | otherwise = field
  where
    needsQuoting =
        T.any (\c -> c == sep || c == '\n' || c == '\r' || c == '"') field

-- | Get a row from the DataFrame with all fields escaped
getRowEscaped :: Char -> DataFrame -> Int -> [T.Text]
getRowEscaped sep df i = V.ifoldr go [] (columns df)
  where
    go :: Int -> Column -> [T.Text] -> [T.Text]
    go _ (BoxedColumn bm (c :: V.Vector a)) acc = case c V.!? i of
        Just e -> escapeField sep textRep : acc
          where
            isNull = case bm of Just bm' -> not (bitmapTestBit bm' i); Nothing -> False
            textRep =
                if isNull
                    then ""
                    else case testEquality (typeRep @a) (typeRep @T.Text) of
                        Just Refl -> e
                        Nothing -> T.pack (show e)
        Nothing -> acc
    go _ (UnboxedColumn bm c) acc = case c VU.!? i of
        Just e ->
            let isNull = case bm of Just bm' -> not (bitmapTestBit bm' i); Nothing -> False
                textRep = if isNull then "" else T.pack (show e)
             in escapeField sep textRep : acc
        Nothing -> acc

testFastCsv :: String -> FilePath -> Test
testFastCsv name csvPath = TestLabel ("fast_roundtrip_" <> name) $ TestCase $ do
    dfOriginal <- D.fastReadCsvUnstable csvPath
    let tempPath = tempDir <> "temp_fast_" <> name <> ".csv"
    prettyPrintCsv tempPath dfOriginal
    dfRoundtrip <- D.fastReadCsvUnstable tempPath
    assertEqual
        ("Fast round-trip should produce equivalent DataFrame for " <> name)
        dfOriginal
        dfRoundtrip
    removeFile tempPath

testTsv :: String -> FilePath -> Test
testTsv name tsvPath = TestLabel ("roundtrip_tsv_" <> name) $ TestCase $ do
    dfOriginal <- D.readTsvUnstable tsvPath
    let tempPath = tempDir <> "temp_" <> name <> ".tsv"
    prettyPrintTsv tempPath dfOriginal
    dfRoundtrip <- D.readTsvUnstable tempPath
    assertEqual
        ("TSV round-trip should produce equivalent DataFrame for " <> name)
        dfOriginal
        dfRoundtrip
    removeFile tempPath

-- Individual round-trip test cases for each fixture

testSimpleFast :: Test
testSimpleFast = testFastCsv "simple" (fixtureDir <> "simple.csv")

testCommaInQuotesFast :: Test
testCommaInQuotesFast = testFastCsv "comma_in_quotes" (fixtureDir <> "comma_in_quotes.csv")

testEscapedQuotesFast :: Test
testEscapedQuotesFast = testFastCsv "escaped_quotes" (fixtureDir <> "escaped_quotes.csv")

testNewlinesFast :: Test
testNewlinesFast = testFastCsv "newlines" (fixtureDir <> "newlines.csv")

testUtf8Fast :: Test
testUtf8Fast = testFastCsv "utf8" (fixtureDir <> "utf8.csv")

testQuotesAndNewlinesFast :: Test
testQuotesAndNewlinesFast = testFastCsv "quotes_and_newlines" (fixtureDir <> "quotes_and_newlines.csv")

testEmptyValuesFast :: Test
testEmptyValuesFast = testFastCsv "empty_values" (fixtureDir <> "empty_values.csv")

testJsonDataFast :: Test
testJsonDataFast = testFastCsv "json_data" (fixtureDir <> "json_data.csv")

arbuthnotPath :: FilePath
arbuthnotPath = "./tests/data/arbuthnot.csv"

-- SpecifyTypes with NoInference fallback: named column is typed, rest stay Text
specifyTypesNoInferenceFallback :: Test
specifyTypesNoInferenceFallback =
    TestLabel "specifyTypes_noInference_fallback" $ TestCase $ do
        df <-
            D.readCsvWithOpts
                D.defaultReadOptions
                    { D.typeSpec =
                        D.SpecifyTypes
                            [("year", D.schemaType @Int)]
                            D.NoInference
                    }
                arbuthnotPath
        -- "year" must be Int
        case getColumn "year" df of
            Just col@(UnboxedColumn _ _) -> assertEqual "year should be Int" "Int" (columnTypeString col)
            _ -> assertFailure "expected UnboxedColumn for 'year'"
        -- "boys" unspecified + NoInference → stays Text
        case getColumn "boys" df of
            Just col@(BoxedColumn _ _) -> assertEqual "boys should be Text" "Text" (columnTypeString col)
            _ -> assertFailure "expected BoxedColumn for 'boys' with NoInference fallback"

-- SpecifyTypes with InferFromSample fallback: named column typed, rest inferred
specifyTypesInferFallback :: Test
specifyTypesInferFallback =
    TestLabel "specifyTypes_inferFromSample_fallback" $ TestCase $ do
        df <-
            D.readCsvWithOpts
                D.defaultReadOptions
                    { D.typeSpec =
                        D.SpecifyTypes
                            [("year", D.schemaType @Int)]
                            (D.InferFromSample 100)
                    }
                arbuthnotPath
        -- "year" must be Int (explicitly specified)
        case getColumn "year" df of
            Just col@(UnboxedColumn _ _) -> assertEqual "year should be Int" "Int" (columnTypeString col)
            _ -> assertFailure "expected UnboxedColumn for 'year'"
        -- "boys" unspecified + InferFromSample → inferred as Int
        case getColumn "boys" df of
            Just col@(UnboxedColumn _ _) -> assertEqual "boys should be Int" "Int" (columnTypeString col)
            _ ->
                assertFailure "expected UnboxedColumn for 'boys' with InferFromSample fallback"

-- SpecifyTypes: typeInferenceSampleSize delegates to fallback
specifyTypesSampleSize :: Test
specifyTypesSampleSize =
    TestLabel "specifyTypes_sampleSize_from_fallback" $ TestCase $ do
        -- Use a small sample size; all numeric columns should still be inferred
        df <-
            D.readCsvWithOpts
                D.defaultReadOptions
                    { D.typeSpec =
                        D.SpecifyTypes
                            []
                            (D.InferFromSample 10)
                    }
                arbuthnotPath
        case getColumn "girls" df of
            Just col@(UnboxedColumn _ _) -> assertEqual "girls should be Int" "Int" (columnTypeString col)
            _ ->
                assertFailure
                    "expected UnboxedColumn for 'girls' via fallback InferFromSample 10"

testCrlfCsv :: Test
testCrlfCsv = TestLabel "malformed_crlf_csv" $ TestCase $ do
    df <- D.readCsvUnstable (fixtureDir <> "crlf.csv")
    let (rows, cols) = dataframeDimensions df
    assertEqual "crlf.csv: 2 data rows" 2 rows
    assertEqual "crlf.csv: 2 columns" 2 cols
    case getColumn "name" df of
        Nothing -> assertFailure "crlf.csv: column 'name' missing"
        Just col ->
            assertEqual
                "crlf.csv: name has no \\r"
                (DI.fromList @T.Text ["Alice", "Bob"])
                col

testCrlfTsv :: Test
testCrlfTsv = TestLabel "malformed_crlf_tsv" $ TestCase $ do
    df <- D.readTsvUnstable (fixtureDir <> "crlf.tsv")
    let (rows, _) = dataframeDimensions df
    assertEqual "crlf.tsv: 1 data row" 1 rows
    case getColumn "name" df of
        Nothing -> assertFailure "crlf.tsv: column 'name' missing"
        Just col ->
            assertEqual
                "crlf.tsv: name has no \\r"
                (DI.fromList @T.Text ["Alice"])
                col

testHeaderOnly :: Test
testHeaderOnly = TestLabel "malformed_header_only" $ TestCase $ do
    df <- D.readCsvUnstable (fixtureDir <> "header_only.csv")
    let (rows, cols) = dataframeDimensions df
    assertEqual "header_only.csv: 0 data rows" 0 rows
    assertEqual "header_only.csv: 3 columns" 3 cols
    let names = map fst . L.sortBy (compare `on` snd) . M.toList $ columnIndices df
    assertEqual "header_only.csv: column names" ["first", "second", "third"] names

testTrailingBlankLine :: Test
testTrailingBlankLine = TestLabel "malformed_trailing_blank_line" $ TestCase $ do
    df <- D.readCsvUnstable (fixtureDir <> "trailing_blank_line.csv")
    -- blank line contributes 1 extra delimiter; (3+3+1) div 3 = 2, numRow=1
    assertEqual
        "trailing_blank_line.csv: 1 data row visible"
        1
        (fst (dataframeDimensions df))

testAllEmptyRow :: Test
testAllEmptyRow = TestLabel "malformed_all_empty_row" $ TestCase $ do
    df <- D.readCsvUnstable (fixtureDir <> "all_empty_row.csv")
    assertEqual "all_empty_row.csv: 1 data row" 1 (fst (dataframeDimensions df))
    let checkEmpty colName =
            case getColumn colName df of
                Nothing -> assertFailure ("column '" <> T.unpack colName <> "' missing")
                Just col ->
                    assertEqual
                        (T.unpack colName <> " is Nothing (empty field → null)")
                        (DI.fromList @(Maybe T.Text) [Nothing])
                        col
    mapM_ checkEmpty ["a", "b", "c"]

testSingleCol :: Test
testSingleCol = TestLabel "malformed_single_col" $ TestCase $ do
    df <- D.readCsvUnstable (fixtureDir <> "single_col.csv")
    let (rows, cols) = dataframeDimensions df
    assertEqual "single_col.csv: 3 data rows" 3 rows
    assertEqual "single_col.csv: 1 column" 1 cols
    case getColumn "name" df of
        Nothing -> assertFailure "single_col.csv: column 'name' missing"
        Just col ->
            assertEqual
                "single_col.csv: correct values"
                (DI.fromList @T.Text ["Alice", "Bob", "Carol"])
                col

testWhitespaceFields :: Test
testWhitespaceFields = TestLabel "malformed_whitespace_fields" $ TestCase $ do
    df <- D.readCsvUnstable (fixtureDir <> "whitespace_fields.csv")
    assertEqual
        "whitespace_fields.csv: 2 data rows"
        2
        (fst (dataframeDimensions df))
    case getColumn "name" df of
        Nothing -> assertFailure "whitespace_fields.csv: 'name' missing"
        Just col -> assertEqual "name stripped" (DI.fromList @T.Text ["Alice", "Bob"]) col
    case getColumn "city" df of
        Nothing -> assertFailure "whitespace_fields.csv: 'city' missing"
        Just col ->
            assertEqual
                "city stripped"
                (DI.fromList @T.Text ["New York", "Los Angeles"])
                col

-- File: a,b,c header; row "1,2" (short); row "X,Y,Z" (full)
-- Total delimiters: 3 (header) + 2 (short) + 3 (full) = 8
-- numCol=3, totalRows=8 div 3=2, numRow=1
-- Row-1 stride offsets 3,4,5 → fields "1","2","X"  (X bleeds in from next row)
testMissingFields :: Test
testMissingFields = TestLabel "malformed_missing_fields" $ TestCase $ do
    df <- D.readCsvUnstable (fixtureDir <> "missing_fields.csv")
    assertEqual
        "missing_fields.csv: integer-division gives 1 visible row"
        1
        (fst (dataframeDimensions df))
    case getColumn "c" df of
        Nothing -> assertFailure "missing_fields.csv: column 'c' missing"
        Just col ->
            -- "X" bleeds from the start of the next row into the missing slot
            assertEqual
                "missing_fields.csv: col 'c' bleeds 'X' from next row"
                (DI.fromList @T.Text ["X"])
                col

-- File: a,b,c header; row "1,2,3,EXTRA"
-- Total delimiters: 3 + 4 = 7; 7 div 3 = 2, numRow=1
-- Row-1 strides 3,4,5 → "1","2","3" — EXTRA (stride 6) is never accessed
testExtraFields :: Test
testExtraFields = TestLabel "malformed_extra_fields" $ TestCase $ do
    df <- readCsvNoInfer (fixtureDir <> "extra_fields.csv")
    assertEqual "extra_fields.csv: 1 data row" 1 (fst (dataframeDimensions df))
    assertEqual "extra_fields.csv: 3 columns" 3 (snd (dataframeDimensions df))
    case getColumn "c" df of
        Nothing -> assertFailure "extra_fields.csv: column 'c' missing"
        Just col ->
            assertEqual
                "extra_fields.csv: 'c' = '3' (EXTRA ignored)"
                (DI.fromList @T.Text ["3"])
                col

testNoTrailingNewline :: Test
testNoTrailingNewline = TestLabel "malformed_no_trailing_newline" $ TestCase $ do
    df <- D.readCsvUnstable (fixtureDir <> "no_trailing_newline.csv")
    assertEqual
        "no_trailing_newline.csv: 1 data row"
        1
        (fst (dataframeDimensions df))
    case getColumn "name" df of
        Nothing -> assertFailure "no_trailing_newline.csv: 'name' missing"
        Just col -> assertEqual "name = Alice" (DI.fromList @T.Text ["Alice"]) col
    case getColumn "city" df of
        Nothing -> assertFailure "no_trailing_newline.csv: 'city' missing"
        Just col ->
            assertEqual
                "city = London (synthetic delimiter worked)"
                (DI.fromList @T.Text ["London"])
                col

tests :: [Test]
tests =
    [ testSimpleFast
    , testCommaInQuotesFast
    , testQuotesAndNewlinesFast
    , testEscapedQuotesFast
    , testNewlinesFast
    , testUtf8Fast
    , testQuotesAndNewlinesFast
    , testEmptyValuesFast
    , testJsonDataFast
    , specifyTypesNoInferenceFallback
    , specifyTypesInferFallback
    , specifyTypesSampleSize
    , testCrlfCsv
    , testCrlfTsv
    , testHeaderOnly
    , testTrailingBlankLine
    , testAllEmptyRow
    , testSingleCol
    , testWhitespaceFields
    , testMissingFields
    , testExtraFields
    , testNoTrailingNewline
    ]
