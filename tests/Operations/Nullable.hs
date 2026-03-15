{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Nullable where

import qualified Data.Vector as V
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.DataFrame as DI
import DataFrame.Internal.Expression (Expr)
import DataFrame.Operators ((.+), (.-), (.==))
import qualified DataFrame.Typed as DT
import qualified DataFrame.Typed.Expr as TE
import DataFrame.Typed.Types (TExpr (..))

import Test.HUnit

-- ---------------------------------------------------------------------------
-- Untyped Expr layer tests
-- ---------------------------------------------------------------------------

-- DataFrame with one plain Int column and one Maybe Int column
testData :: D.DataFrame
testData =
    D.fromNamedColumns
        [ ("x", DI.fromList ([1, 2, 3] :: [Int]))
        , ("y", DI.OptionalColumn (V.fromList [Just 10, Nothing, Just 30 :: Maybe Int]))
        ]

-- | col @Int .+ col @(Maybe Int)  should give Maybe Int column
addIntMaybeInt :: Test
addIntMaybeInt =
    TestCase
        ( assertEqual
            "Int .+ Maybe Int = Maybe Int"
            (Just $ DI.OptionalColumn (V.fromList [Just 11, Nothing, Just 33 :: Maybe Int]))
            ( DI.getColumn "result" $
                D.derive
                    "result"
                    (F.col @Int "x" .+ F.col @(Maybe Int) "y")
                    testData
            )
        )

-- | col @(Maybe Int) .+ col @Int  should give Maybe Int column
addMaybeIntInt :: Test
addMaybeIntInt =
    TestCase
        ( assertEqual
            "Maybe Int .+ Int = Maybe Int"
            (Just $ DI.OptionalColumn (V.fromList [Just 11, Nothing, Just 33 :: Maybe Int]))
            ( DI.getColumn "result" $
                D.derive
                    "result"
                    (F.col @(Maybe Int) "y" .+ F.col @Int "x")
                    testData
            )
        )

-- | col @Int .+ col @Int  (same-type non-nullable) should give Int column
addIntInt :: Test
addIntInt =
    TestCase
        ( assertEqual
            "Int .+ Int = Int (non-nullable preserved)"
            (Just $ DI.fromList [2, 4, 6 :: Int])
            ( DI.getColumn "result" $
                D.derive
                    "result"
                    (F.col @Int "x" .+ F.col @Int "x")
                    testData
            )
        )

-- | col @(Maybe Int) .+ col @(Maybe Int)  should give Maybe Int column
addMaybeMaybe :: Test
addMaybeMaybe =
    TestCase
        ( assertEqual
            "Maybe Int .+ Maybe Int = Maybe Int"
            (Just $ DI.OptionalColumn (V.fromList [Just 20, Nothing, Just 60 :: Maybe Int]))
            ( DI.getColumn "result" $
                D.derive
                    "result"
                    (F.col @(Maybe Int) "y" .+ F.col @(Maybe Int) "y")
                    testData
            )
        )

-- | Subtraction: Int .- Maybe Int
subIntMaybeInt :: Test
subIntMaybeInt =
    TestCase
        ( assertEqual
            "Int .- Maybe Int = Maybe Int"
            ( Just $
                DI.OptionalColumn (V.fromList [Just (-9), Nothing, Just (-27) :: Maybe Int])
            )
            ( DI.getColumn "result" $
                D.derive
                    "result"
                    (F.col @Int "x" .- F.col @(Maybe Int) "y")
                    testData
            )
        )

-- | Comparison: Int .== Maybe Int gives Maybe Bool
eqIntMaybeInt :: Test
eqIntMaybeInt =
    TestCase
        ( assertEqual
            "Int .== Maybe Int = Maybe Bool"
            ( Just $
                DI.OptionalColumn (V.fromList [Just False, Nothing, Just False :: Maybe Bool])
            )
            ( DI.getColumn "result" $
                D.derive
                    "result"
                    (F.col @Int "x" .== F.col @(Maybe Int) "y" :: Expr (Maybe Bool))
                    testData
            )
        )

-- | Comparison: Int .== Int  gives plain Bool (backwards compatible)
eqIntInt :: Test
eqIntInt =
    TestCase
        ( assertEqual
            "Int .== Int = Bool (non-nullable, backwards compatible)"
            (Just $ DI.fromList [True, False, False :: Bool])
            ( DI.getColumn "result" $
                D.derive
                    "result"
                    (F.col @Int "x" .== F.lit (1 :: Int) :: Expr Bool)
                    testData
            )
        )

-- ---------------------------------------------------------------------------
-- nullLift / nullLift2 — untyped layer
-- ---------------------------------------------------------------------------

-- | nullLift negate on Maybe Int propagates Nothing
nullLiftMaybeInt :: Test
nullLiftMaybeInt =
    TestCase
        ( assertEqual
            "nullLift negate (Maybe Int) propagates Nothing"
            ( Just $
                DI.OptionalColumn (V.fromList [Just (-10), Nothing, Just (-30) :: Maybe Int])
            )
            ( DI.getColumn "result" $
                D.derive
                    "result"
                    (F.nullLift negate (F.col @(Maybe Int) "y") :: Expr (Maybe Int))
                    testData
            )
        )

-- | nullLift negate on plain Int — no Nothing, plain result
nullLiftInt :: Test
nullLiftInt =
    TestCase
        ( assertEqual
            "nullLift negate (Int) gives Int column"
            (Just $ DI.fromList [-1, -2, -3 :: Int])
            ( DI.getColumn "result" $
                D.derive
                    "result"
                    (F.nullLift negate (F.col @Int "x") :: Expr Int)
                    testData
            )
        )

-- | nullLift2 (+) Int (Maybe Int) → Maybe Int
nullLift2IntMaybeInt :: Test
nullLift2IntMaybeInt =
    TestCase
        ( assertEqual
            "nullLift2 (+) Int (Maybe Int) = Maybe Int"
            (Just $ DI.OptionalColumn (V.fromList [Just 11, Nothing, Just 33 :: Maybe Int]))
            ( DI.getColumn "result" $
                D.derive
                    "result"
                    (F.nullLift2 (+) (F.col @Int "x") (F.col @(Maybe Int) "y") :: Expr (Maybe Int))
                    testData
            )
        )

-- | nullLift2 (+) (Maybe Int) Int → Maybe Int
nullLift2MaybeIntInt :: Test
nullLift2MaybeIntInt =
    TestCase
        ( assertEqual
            "nullLift2 (+) (Maybe Int) Int = Maybe Int"
            (Just $ DI.OptionalColumn (V.fromList [Just 11, Nothing, Just 33 :: Maybe Int]))
            ( DI.getColumn "result" $
                D.derive
                    "result"
                    (F.nullLift2 (+) (F.col @(Maybe Int) "y") (F.col @Int "x") :: Expr (Maybe Int))
                    testData
            )
        )

-- | nullLift2 (+) Int Int → plain Int
nullLift2IntInt :: Test
nullLift2IntInt =
    TestCase
        ( assertEqual
            "nullLift2 (+) Int Int = Int (non-nullable)"
            (Just $ DI.fromList [2, 4, 6 :: Int])
            ( DI.getColumn "result" $
                D.derive
                    "result"
                    (F.nullLift2 (+) (F.col @Int "x") (F.col @Int "x") :: Expr Int)
                    testData
            )
        )

-- ---------------------------------------------------------------------------
-- Typed TExpr layer tests
-- ---------------------------------------------------------------------------

type TestSchema = '[DT.Column "x" Int, DT.Column "y" (Maybe Int)]

typedTestData :: DT.TypedDataFrame TestSchema
typedTestData =
    either (error . show) id $
        DT.freezeWithError @TestSchema testData

-- | Typed: col @"x" .+ col @"y"  should give Maybe Int column
typedAddIntMaybeInt :: Test
typedAddIntMaybeInt =
    TestCase
        ( assertEqual
            "Typed: Int .+ Maybe Int = Maybe Int"
            [Just 11, Nothing, Just 33]
            ( DT.columnAsList @"result" $
                DT.derive
                    @"result"
                    (TE.col @"x" TE..+ TE.col @"y")
                    typedTestData
            )
        )

-- | Typed: nullLift negate on Maybe Int propagates Nothing
typedNullLiftMaybeInt :: Test
typedNullLiftMaybeInt =
    TestCase
        ( assertEqual
            "Typed nullLift negate (Maybe Int) propagates Nothing"
            [Just (-10), Nothing, Just (-30 :: Int)]
            ( DT.columnAsList @"result" $
                DT.derive
                    @"result"
                    (TE.nullLift negate (TE.col @"y") :: TExpr TestSchema (Maybe Int))
                    typedTestData
            )
        )

-- | Typed: nullLift negate on plain Int
typedNullLiftInt :: Test
typedNullLiftInt =
    TestCase
        ( assertEqual
            "Typed nullLift negate (Int) gives Int column"
            [-1, -2, -3 :: Int]
            ( DT.columnAsList @"result" $
                DT.derive
                    @"result"
                    (TE.nullLift negate (TE.col @"x") :: TExpr TestSchema Int)
                    typedTestData
            )
        )

-- | Typed: nullLift2 (+) col @"x" col @"y" → Maybe Int
typedNullLift2IntMaybeInt :: Test
typedNullLift2IntMaybeInt =
    TestCase
        ( assertEqual
            "Typed nullLift2 (+) Int (Maybe Int) = Maybe Int"
            [Just 11, Nothing, Just 33 :: Maybe Int]
            ( DT.columnAsList @"result" $
                DT.derive
                    @"result"
                    (TE.nullLift2 (+) (TE.col @"x") (TE.col @"y") :: TExpr TestSchema (Maybe Int))
                    typedTestData
            )
        )

-- | Typed: col @"y" .== col @"y"  should give Maybe Bool column
typedEqMaybeMaybe :: Test
typedEqMaybeMaybe =
    TestCase
        ( assertEqual
            "Typed: Maybe Int .== Maybe Int = Maybe Bool"
            [Just True, Nothing, Just True]
            ( DT.columnAsList @"result" $
                DT.derive
                    @"result"
                    (TE.col @"y" TE..== TE.col @"y")
                    typedTestData
            )
        )

-- | apply negate to a Maybe Int column — should fmap over inner values
applyFmapMaybeInt :: Test
applyFmapMaybeInt =
    TestCase
        ( assertEqual
            "apply negate to Maybe Int column fmaps"
            (Just $ DI.OptionalColumn (V.fromList [Just (-10), Nothing, Just (-30 :: Int)]))
            ( DI.getColumn "y" $
                D.apply @Int negate "y" testData
            )
        )

-- | apply to a plain Int column still works
applyPlainInt :: Test
applyPlainInt =
    TestCase
        ( assertEqual
            "apply negate to plain Int column still works"
            (Just $ DI.fromList [-1, -2, -3 :: Int])
            ( DI.getColumn "x" $
                D.apply @Int negate "x" testData
            )
        )

tests :: [Test]
tests =
    [ TestLabel "addIntMaybeInt" addIntMaybeInt
    , TestLabel "addMaybeIntInt" addMaybeIntInt
    , TestLabel "addIntInt" addIntInt
    , TestLabel "addMaybeMaybe" addMaybeMaybe
    , TestLabel "subIntMaybeInt" subIntMaybeInt
    , TestLabel "eqIntMaybeInt" eqIntMaybeInt
    , TestLabel "eqIntInt" eqIntInt
    , TestLabel "nullLiftMaybeInt" nullLiftMaybeInt
    , TestLabel "nullLiftInt" nullLiftInt
    , TestLabel "nullLift2IntMaybeInt" nullLift2IntMaybeInt
    , TestLabel "nullLift2MaybeIntInt" nullLift2MaybeIntInt
    , TestLabel "nullLift2IntInt" nullLift2IntInt
    , TestLabel "typedAddIntMaybeInt" typedAddIntMaybeInt
    , TestLabel "typedEqMaybeMaybe" typedEqMaybeMaybe
    , TestLabel "typedNullLiftMaybeInt" typedNullLiftMaybeInt
    , TestLabel "typedNullLiftInt" typedNullLiftInt
    , TestLabel "typedNullLift2IntMaybeInt" typedNullLift2IntMaybeInt
    , TestLabel "applyFmapMaybeInt" applyFmapMaybeInt
    , TestLabel "applyPlainInt" applyPlainInt
    ]
