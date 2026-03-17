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
import DataFrame.Internal.Column (Column (..), columnTypeString)
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
    go _ (BoxedColumn (c :: V.Vector a)) acc = case c V.!? i of
        Just e -> escapeField sep textRep : acc
          where
            textRep = case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl -> e
                Nothing -> T.pack (show e)
        Nothing -> acc
    go _ (UnboxedColumn c) acc = case c VU.!? i of
        Just e -> escapeField sep (T.pack (show e)) : acc
        Nothing -> acc
    go _ (OptionalColumn (c :: V.Vector (Maybe a))) acc = case c V.!? i of
        Just e -> escapeField sep textRep : acc
          where
            textRep = case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl -> fromMaybe "" e
                Nothing -> case e of
                    Just val -> T.pack (show val)
                    Nothing -> ""
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
            Just col@(UnboxedColumn _) -> assertEqual "year should be Int" "Int" (columnTypeString col)
            _ -> assertFailure "expected UnboxedColumn for 'year'"
        -- "boys" unspecified + NoInference → stays Text
        case getColumn "boys" df of
            Just col@(BoxedColumn _) -> assertEqual "boys should be Text" "Text" (columnTypeString col)
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
            Just col@(UnboxedColumn _) -> assertEqual "year should be Int" "Int" (columnTypeString col)
            _ -> assertFailure "expected UnboxedColumn for 'year'"
        -- "boys" unspecified + InferFromSample → inferred as Int
        case getColumn "boys" df of
            Just col@(UnboxedColumn _) -> assertEqual "boys should be Int" "Int" (columnTypeString col)
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
            Just col@(UnboxedColumn _) -> assertEqual "girls should be Int" "Int" (columnTypeString col)
            _ ->
                assertFailure
                    "expected UnboxedColumn for 'girls' via fallback InferFromSample 10"

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
    ]
