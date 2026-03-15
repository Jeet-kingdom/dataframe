{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

{- | Type-safe expression construction for typed DataFrames.

Unlike the untyped @Expr a@ where column references are unchecked strings,
'TExpr' ensures at compile time that:

* Referenced columns exist in the schema
* Column types match the expression type

== Example

@
type Schema = '[Column \"age\" Int, Column \"salary\" Double]

-- This compiles:
goodExpr :: TExpr Schema Double
goodExpr = col \@\"salary\"

-- This gives a compile-time error (column not found):
badExpr :: TExpr Schema Double
badExpr = col \@\"nonexistent\"

-- This gives a compile-time error (type mismatch):
wrongType :: TExpr Schema Int
wrongType = col \@\"salary\"  -- salary is Double, not Int
@
-}
module DataFrame.Typed.Expr (
    -- * Core typed expression type (re-exported from Types)
    TExpr (..),

    -- * Column reference (schema-checked)
    col,

    -- * Literals
    lit,

    -- * Conditional
    ifThenElse,

    -- * Unary / binary lifting
    lift,
    lift2,
    nullLift,
    nullLift2,

    -- * Same-type comparison operators
    (.==.),
    (./=.),
    (.<.),
    (.<=.),
    (.>=.),
    (.>.),

    -- * Nullable-aware arithmetic operators
    (.+),
    (.-),
    (.*),
    (./),

    -- * Nullable-aware comparison operators (three-valued logic)
    (.==),
    (./=),
    (.<),
    (.<=),
    (.>=),
    (.>),

    -- * Logical operators
    (.&&.),
    (.||.),
    DataFrame.Typed.Expr.not,

    -- * Aggregation combinators
    sum,
    mean,
    count,
    minimum,
    maximum,
    collect,

    -- * Named expression helper
    as,

    -- * Sort helpers
    asc,
    desc,
) where

import Data.Proxy (Proxy (..))
import Data.String (IsString (..))
import qualified Data.Text as T
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)

import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (
    AggStrategy (..),
    BinaryOp (..),
    Expr (..),
    MeanAcc (..),
    NamedExpr,
    UExpr (..),
    UnaryOp (..),
 )
import DataFrame.Internal.Nullable (
    BaseType,
    NullCmpResult,
    NullLift1Op (applyNull1),
    NullLift1Result,
    NullLift2Op (applyNull2),
    NullLift2Result,
    NullableArithOp (nullArithOp),
    NullableCmpOp (nullCmpOp),
 )

import DataFrame.Typed.Schema (AssertPresent, Lookup)
import DataFrame.Typed.Types (TExpr (..), TSortOrder (..))
import Prelude hiding (maximum, minimum, sum)

{- | Create a typed column reference. This is the key type-safety entry point.

The column name must exist in @cols@ and its type must match @a@.
Both checks happen at compile time via type families.

@
salary :: TExpr '[Column \"salary\" Double] Double
salary = col \@\"salary\"
@
-}
col ::
    forall (name :: Symbol) cols a.
    ( KnownSymbol name
    , a ~ Lookup name cols
    , Columnable a
    , AssertPresent name cols
    ) =>
    TExpr cols a
col = TExpr (Col (T.pack (symbolVal (Proxy @name))))

{- | Create a literal expression. Valid for any schema since it
references no columns.
-}
lit :: (Columnable a) => a -> TExpr cols a
lit = TExpr . Lit

-- | Conditional expression.
ifThenElse ::
    (Columnable a) =>
    TExpr cols Bool -> TExpr cols a -> TExpr cols a -> TExpr cols a
ifThenElse (TExpr c) (TExpr t) (TExpr e) = TExpr (If c t e)

-------------------------------------------------------------------------------
-- Numeric instances (mirror Expr's instances)
-------------------------------------------------------------------------------

instance (Num a, Columnable a) => Num (TExpr cols a) where
    (TExpr a) + (TExpr b) = TExpr (a + b)
    (TExpr a) - (TExpr b) = TExpr (a - b)
    (TExpr a) * (TExpr b) = TExpr (a * b)
    negate (TExpr a) = TExpr (negate a)
    abs (TExpr a) = TExpr (abs a)
    signum (TExpr a) = TExpr (signum a)
    fromInteger = TExpr . fromInteger

instance (Fractional a, Columnable a) => Fractional (TExpr cols a) where
    fromRational = TExpr . fromRational
    (TExpr a) / (TExpr b) = TExpr (a / b)

instance (Floating a, Columnable a) => Floating (TExpr cols a) where
    pi = TExpr pi
    exp (TExpr a) = TExpr (exp a)
    sqrt (TExpr a) = TExpr (sqrt a)
    log (TExpr a) = TExpr (log a)
    (TExpr a) ** (TExpr b) = TExpr (a ** b)
    logBase (TExpr a) (TExpr b) = TExpr (logBase a b)
    sin (TExpr a) = TExpr (sin a)
    cos (TExpr a) = TExpr (cos a)
    tan (TExpr a) = TExpr (tan a)
    asin (TExpr a) = TExpr (asin a)
    acos (TExpr a) = TExpr (acos a)
    atan (TExpr a) = TExpr (atan a)
    sinh (TExpr a) = TExpr (sinh a)
    cosh (TExpr a) = TExpr (cosh a)
    asinh (TExpr a) = TExpr (asinh a)
    acosh (TExpr a) = TExpr (acosh a)
    atanh (TExpr a) = TExpr (atanh a)

instance (IsString a, Columnable a) => IsString (TExpr cols a) where
    fromString = TExpr . fromString

-------------------------------------------------------------------------------
-- Lifting arbitrary functions
-------------------------------------------------------------------------------

-- | Lift a unary function into a typed expression.
lift ::
    (Columnable a, Columnable b) => (a -> b) -> TExpr cols a -> TExpr cols b
lift f (TExpr e) = TExpr (Unary (MkUnaryOp f "unaryUdf" Nothing) e)

-- | Lift a binary function into typed expressions.
lift2 ::
    (Columnable a, Columnable b, Columnable c) =>
    (a -> b -> c) -> TExpr cols a -> TExpr cols b -> TExpr cols c
lift2 f (TExpr a) (TExpr b) = TExpr (Binary (MkBinaryOp f "binaryUdf" Nothing False 0) a b)

{- | Typed 'nullLift': lift a unary function with nullable propagation.
When the input is @Maybe a@, 'Nothing' short-circuits; when plain @a@, applies directly.
The return type is inferred via 'NullLift1Result': no annotation needed.
-}
nullLift ::
    (NullLift1Op a r (NullLift1Result a r), Columnable (NullLift1Result a r)) =>
    (BaseType a -> r) ->
    TExpr cols a ->
    TExpr cols (NullLift1Result a r)
nullLift f (TExpr e) = TExpr (Unary (MkUnaryOp (applyNull1 f) "nullLift" Nothing) e)

{- | Typed 'nullLift2': lift a binary function with nullable propagation.
Any 'Nothing' operand short-circuits to 'Nothing' in the result.
The return type is inferred via 'NullLift2Result': no annotation needed.
-}
nullLift2 ::
    (NullLift2Op a b r (NullLift2Result a b r), Columnable (NullLift2Result a b r)) =>
    (BaseType a -> BaseType b -> r) ->
    TExpr cols a ->
    TExpr cols b ->
    TExpr cols (NullLift2Result a b r)
nullLift2 f (TExpr a) (TExpr b) = TExpr (Binary (MkBinaryOp (applyNull2 f) "nullLift2" Nothing False 0) a b)

infixl 4 .==., ./=., .<., .<=., .>=., .>.
infixr 3 .&&.
infixr 2 .||.

(.==.) ::
    (Columnable a, Eq a) => TExpr cols a -> TExpr cols a -> TExpr cols Bool
(.==.) (TExpr a) (TExpr b) = TExpr (Binary (MkBinaryOp (==) "eq" (Just "==") True 4) a b)

(./=.) ::
    (Columnable a, Eq a) => TExpr cols a -> TExpr cols a -> TExpr cols Bool
(./=.) (TExpr a) (TExpr b) = TExpr (Binary (MkBinaryOp (/=) "neq" (Just "/=") True 4) a b)

(.<.) ::
    (Columnable a, Ord a) => TExpr cols a -> TExpr cols a -> TExpr cols Bool
(.<.) (TExpr a) (TExpr b) = TExpr (Binary (MkBinaryOp (<) "lt" (Just "<") False 4) a b)

(.<=.) ::
    (Columnable a, Ord a) => TExpr cols a -> TExpr cols a -> TExpr cols Bool
(.<=.) (TExpr a) (TExpr b) = TExpr (Binary (MkBinaryOp (<=) "leq" (Just "<=") False 4) a b)

(.>=.) ::
    (Columnable a, Ord a) => TExpr cols a -> TExpr cols a -> TExpr cols Bool
(.>=.) (TExpr a) (TExpr b) = TExpr (Binary (MkBinaryOp (>=) "geq" (Just ">=") False 4) a b)

(.>.) ::
    (Columnable a, Ord a) => TExpr cols a -> TExpr cols a -> TExpr cols Bool
(.>.) (TExpr a) (TExpr b) = TExpr (Binary (MkBinaryOp (>) "gt" (Just ">") False 4) a b)

(.&&.) :: TExpr cols Bool -> TExpr cols Bool -> TExpr cols Bool
(.&&.) (TExpr a) (TExpr b) = TExpr (Binary (MkBinaryOp (&&) "and" (Just "&&") True 3) a b)

(.||.) :: TExpr cols Bool -> TExpr cols Bool -> TExpr cols Bool
(.||.) (TExpr a) (TExpr b) = TExpr (Binary (MkBinaryOp (||) "or" (Just "||") True 2) a b)

-------------------------------------------------------------------------------
-- Nullable-aware arithmetic operators
-------------------------------------------------------------------------------

infixl 6 .+, .-
infixl 7 .*, ./
infix 4 .==, ./=, .<, .<=, .>=, .>

{- | Nullable-aware addition. Works for all combinations of nullable\/non-nullable operands.
@col \@\"x\" '.+' col \@\"y\"  -- :: TExpr cols (Maybe Int)  when y :: Maybe Int@
-}
(.+) ::
    (NullableArithOp a b c, Num (BaseType a)) =>
    TExpr cols a ->
    TExpr cols b ->
    TExpr cols c
(.+) (TExpr a) (TExpr b) =
    TExpr (Binary (MkBinaryOp (nullArithOp (+)) "nulladd" (Just "+") True 6) a b)

-- | Nullable-aware subtraction.
(.-) ::
    (NullableArithOp a b c, Num (BaseType a)) =>
    TExpr cols a ->
    TExpr cols b ->
    TExpr cols c
(.-) (TExpr a) (TExpr b) =
    TExpr (Binary (MkBinaryOp (nullArithOp (-)) "nullsub" (Just "-") False 6) a b)

-- | Nullable-aware multiplication.
(.*) ::
    (NullableArithOp a b c, Num (BaseType a)) =>
    TExpr cols a ->
    TExpr cols b ->
    TExpr cols c
(.*) (TExpr a) (TExpr b) =
    TExpr (Binary (MkBinaryOp (nullArithOp (*)) "nullmul" (Just "*") True 7) a b)

-- | Nullable-aware division.
(./) ::
    (NullableArithOp a b c, Fractional (BaseType a)) =>
    TExpr cols a ->
    TExpr cols b ->
    TExpr cols c
(./) (TExpr a) (TExpr b) =
    TExpr (Binary (MkBinaryOp (nullArithOp (/)) "nulldiv" (Just "/") False 7) a b)

-------------------------------------------------------------------------------
-- Nullable-aware comparison operators (three-valued logic)
-------------------------------------------------------------------------------

-- | Nullable-aware equality. Returns @Maybe Bool@ when either operand is nullable.
(.==) ::
    (NullableCmpOp a b (NullCmpResult a b), Eq (BaseType a)) =>
    TExpr cols a ->
    TExpr cols b ->
    TExpr cols (NullCmpResult a b)
(.==) (TExpr a) (TExpr b) =
    TExpr (Binary (MkBinaryOp (nullCmpOp (==)) "eq" (Just "==") True 4) a b)

-- | Nullable-aware inequality.
(./=) ::
    (NullableCmpOp a b (NullCmpResult a b), Eq (BaseType a)) =>
    TExpr cols a ->
    TExpr cols b ->
    TExpr cols (NullCmpResult a b)
(./=) (TExpr a) (TExpr b) =
    TExpr (Binary (MkBinaryOp (nullCmpOp (/=)) "neq" (Just "/=") True 4) a b)

-- | Nullable-aware less-than.
(.<) ::
    (NullableCmpOp a b (NullCmpResult a b), Ord (BaseType a)) =>
    TExpr cols a ->
    TExpr cols b ->
    TExpr cols (NullCmpResult a b)
(.<) (TExpr a) (TExpr b) =
    TExpr (Binary (MkBinaryOp (nullCmpOp (<)) "lt" (Just "<") False 4) a b)

-- | Nullable-aware less-than-or-equal.
(.<=) ::
    (NullableCmpOp a b (NullCmpResult a b), Ord (BaseType a)) =>
    TExpr cols a ->
    TExpr cols b ->
    TExpr cols (NullCmpResult a b)
(.<=) (TExpr a) (TExpr b) =
    TExpr (Binary (MkBinaryOp (nullCmpOp (<=)) "leq" (Just "<=") False 4) a b)

-- | Nullable-aware greater-than-or-equal.
(.>=) ::
    (NullableCmpOp a b (NullCmpResult a b), Ord (BaseType a)) =>
    TExpr cols a ->
    TExpr cols b ->
    TExpr cols (NullCmpResult a b)
(.>=) (TExpr a) (TExpr b) =
    TExpr (Binary (MkBinaryOp (nullCmpOp (>=)) "geq" (Just ">=") False 4) a b)

-- | Nullable-aware greater-than.
(.>) ::
    (NullableCmpOp a b (NullCmpResult a b), Ord (BaseType a)) =>
    TExpr cols a ->
    TExpr cols b ->
    TExpr cols (NullCmpResult a b)
(.>) (TExpr a) (TExpr b) =
    TExpr (Binary (MkBinaryOp (nullCmpOp (>)) "gt" (Just ">") False 4) a b)

not :: TExpr cols Bool -> TExpr cols Bool
not (TExpr e) = TExpr (Unary (MkUnaryOp Prelude.not "not" (Just "!")) e)

-------------------------------------------------------------------------------
-- Aggregation combinators
-------------------------------------------------------------------------------

sum :: (Columnable a, Num a) => TExpr cols a -> TExpr cols a
sum (TExpr e) = TExpr (Agg (FoldAgg "sum" Nothing (+)) e)

mean :: (Columnable a, Real a) => TExpr cols a -> TExpr cols Double
mean (TExpr e) =
    TExpr
        ( Agg
            ( MergeAgg
                "mean"
                (MeanAcc 0.0 0)
                (\(MeanAcc s c) x -> MeanAcc (s + realToFrac x) (c + 1))
                (\(MeanAcc s1 c1) (MeanAcc s2 c2) -> MeanAcc (s1 + s2) (c1 + c2))
                (\(MeanAcc s c) -> if c == 0 then 0 / 0 else s / fromIntegral c)
            )
            e
        )

count :: (Columnable a) => TExpr cols a -> TExpr cols Int
count (TExpr e) = TExpr (Agg (MergeAgg "count" (0 :: Int) (\c _ -> c + 1) (+) id) e)

minimum :: (Columnable a, Ord a) => TExpr cols a -> TExpr cols a
minimum (TExpr e) = TExpr (Agg (FoldAgg "minimum" Nothing min) e)

maximum :: (Columnable a, Ord a) => TExpr cols a -> TExpr cols a
maximum (TExpr e) = TExpr (Agg (FoldAgg "maximum" Nothing max) e)

collect :: (Columnable a) => TExpr cols a -> TExpr cols [a]
collect (TExpr e) = TExpr (Agg (FoldAgg "collect" (Just []) (flip (:))) e)

-------------------------------------------------------------------------------
-- Named expression helper
-------------------------------------------------------------------------------

-- | Create a 'NamedExpr' for use with 'aggregateUntyped'.
as :: (Columnable a) => TExpr cols a -> T.Text -> NamedExpr
as (TExpr e) name = (name, UExpr e)

-- | Create an ascending sort order from a typed expression.
asc :: (Columnable a) => TExpr cols a -> TSortOrder cols
asc = Asc

-- | Create a descending sort order from a typed expression.
desc :: (Columnable a) => TExpr cols a -> TSortOrder cols
desc = Desc
