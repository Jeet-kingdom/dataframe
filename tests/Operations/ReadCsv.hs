{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Operations.ReadCsv where

import qualified Data.Text as T
import qualified DataFrame as D

import DataFrame.Internal.Column (Column (..), columnTypeString)
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    dataframeDimensions,
    getColumn,
 )
import Test.HUnit

arbuthnotPath :: FilePath
arbuthnotPath = "./tests/data/arbuthnot.csv"

readCsvNoInfer :: FilePath -> IO DataFrame
readCsvNoInfer =
    D.readCsvWithOpts
        D.defaultReadOptions{D.typeSpec = D.NoInference}

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

-- File: a,b,c header; row "1,2,3,EXTRA"
-- Total delimiters: 3 + 4 = 7; 7 div 3 = 2, numRow=1
-- Row-1 strides 3,4,5 → "1","2","3" — EXTRA (stride 6) is never accessed
testExtraFields :: Test
testExtraFields = TestLabel "malformed_extra_fields" $ TestCase $ do
    df <- readCsvNoInfer ("./tests/data/unstable_csv/" <> "extra_fields.csv")
    assertEqual "extra_fields.csv: 1 data row" 1 (fst (dataframeDimensions df))
    assertEqual "extra_fields.csv: 3 columns" 3 (snd (dataframeDimensions df))
    case getColumn "c" df of
        Nothing -> assertFailure "extra_fields.csv: column 'c' missing"
        Just col ->
            assertEqual
                "extra_fields.csv: 'c' = '3' (EXTRA ignored)"
                (D.fromList @T.Text ["3"])
                col

tests :: [Test]
tests =
    [ specifyTypesNoInferenceFallback
    , specifyTypesInferFallback
    , specifyTypesSampleSize
    , testExtraFields
    ]
