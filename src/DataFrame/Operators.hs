{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module DataFrame.Operators where

import Data.Function ((&))
import qualified Data.Text as T
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (
    BinaryOp (
        MkBinaryOp,
        binaryCommutative,
        binaryFn,
        binaryName,
        binaryPrecedence,
        binarySymbol
    ),
    Expr (Binary, Col, If, Lit),
    NamedExpr,
    UExpr (UExpr),
 )
import DataFrame.Internal.Nullable (
    BaseType,
    NullCmpResult,
    NullableArithOp (nullArithOp),
    NullableCmpOp (nullCmpOp),
 )

infix 8 .^^
infix 6 .+, .-
infix 7 .*, ./
infix 4 .==, .<, .<=, .>=, .>, ./=
infixr 3 .&&
infixr 2 .||
infixr 0 .=

(|>) :: a -> (a -> b) -> b
(|>) = (&)

as :: (Columnable a) => Expr a -> T.Text -> NamedExpr
as expr name = (name, UExpr expr)

name :: (Show a) => Expr a -> T.Text
name (Col n) = n
name other =
    error $
        "You must call `name` on a column reference. Not the expression: " ++ show other

col :: (Columnable a) => T.Text -> Expr a
col = Col

ifThenElse :: (Columnable a) => Expr Bool -> Expr a -> Expr a -> Expr a
ifThenElse = If

lit :: (Columnable a) => a -> Expr a
lit = Lit

(.=) :: (Columnable a) => T.Text -> Expr a -> NamedExpr
(.=) = flip as

-- Nullable-aware arithmetic operators

{- | Nullable-aware addition. Works for all combinations of nullable\/non-nullable operands.
@col \@Int "x" .+ col \@(Maybe Int) "y"  -- :: Expr (Maybe Int)@
-}
(.+) ::
    (NullableArithOp a b c, Num (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr c
(.+) =
    Binary
        ( MkBinaryOp
            { binaryFn = nullArithOp (+)
            , binaryName = "nulladd"
            , binarySymbol = Just "+"
            , binaryCommutative = True
            , binaryPrecedence = 6
            }
        )

-- | Nullable-aware subtraction.
(.-) ::
    (NullableArithOp a b c, Num (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr c
(.-) =
    Binary
        ( MkBinaryOp
            { binaryFn = nullArithOp (-)
            , binaryName = "nullsub"
            , binarySymbol = Just "-"
            , binaryCommutative = False
            , binaryPrecedence = 6
            }
        )

-- | Nullable-aware multiplication.
(.*) ::
    (NullableArithOp a b c, Num (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr c
(.*) =
    Binary
        ( MkBinaryOp
            { binaryFn = nullArithOp (*)
            , binaryName = "nullmul"
            , binarySymbol = Just "*"
            , binaryCommutative = True
            , binaryPrecedence = 7
            }
        )

-- | Nullable-aware division.
(./) ::
    (NullableArithOp a b c, Fractional (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr c
(./) =
    Binary
        ( MkBinaryOp
            { binaryFn = nullArithOp (/)
            , binaryName = "nulldiv"
            , binarySymbol = Just "/"
            , binaryCommutative = False
            , binaryPrecedence = 7
            }
        )

-- Nullable-aware comparison operators (three-valued logic: Nothing if either operand is Nothing)

-- | Nullable-aware equality. Returns @Maybe Bool@ when either operand is nullable.
(.==) ::
    (NullableCmpOp a b (NullCmpResult a b), Eq (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.==) =
    Binary
        ( MkBinaryOp
            { binaryFn = nullCmpOp (==)
            , binaryName = "eq"
            , binarySymbol = Just "=="
            , binaryCommutative = True
            , binaryPrecedence = 4
            }
        )

-- | Nullable-aware inequality.
(./=) ::
    (NullableCmpOp a b (NullCmpResult a b), Eq (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(./=) =
    Binary
        ( MkBinaryOp
            { binaryFn = nullCmpOp (/=)
            , binaryName = "neq"
            , binarySymbol = Just "/="
            , binaryCommutative = True
            , binaryPrecedence = 4
            }
        )

-- | Nullable-aware less-than.
(.<) ::
    (NullableCmpOp a b (NullCmpResult a b), Ord (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.<) =
    Binary
        ( MkBinaryOp
            { binaryFn = nullCmpOp (<)
            , binaryName = "lt"
            , binarySymbol = Just "<"
            , binaryCommutative = False
            , binaryPrecedence = 4
            }
        )

-- | Nullable-aware greater-than.
(.>) ::
    (NullableCmpOp a b (NullCmpResult a b), Ord (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.>) =
    Binary
        ( MkBinaryOp
            { binaryFn = nullCmpOp (>)
            , binaryName = "gt"
            , binarySymbol = Just ">"
            , binaryCommutative = False
            , binaryPrecedence = 4
            }
        )

-- | Nullable-aware less-than-or-equal.
(.<=) ::
    (NullableCmpOp a b (NullCmpResult a b), Ord (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.<=) =
    Binary
        ( MkBinaryOp
            { binaryFn = nullCmpOp (<=)
            , binaryName = "leq"
            , binarySymbol = Just "<="
            , binaryCommutative = False
            , binaryPrecedence = 4
            }
        )

-- | Nullable-aware greater-than-or-equal.
(.>=) ::
    (NullableCmpOp a b (NullCmpResult a b), Ord (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.>=) =
    Binary
        ( MkBinaryOp
            { binaryFn = nullCmpOp (>=)
            , binaryName = "geq"
            , binarySymbol = Just ">="
            , binaryCommutative = False
            , binaryPrecedence = 4
            }
        )

(.&&) :: Expr Bool -> Expr Bool -> Expr Bool
(.&&) =
    Binary
        ( MkBinaryOp
            { binaryFn = (&&)
            , binaryName = "and"
            , binarySymbol = Just "&&"
            , binaryCommutative = True
            , binaryPrecedence = 3
            }
        )

(.||) :: Expr Bool -> Expr Bool -> Expr Bool
(.||) =
    Binary
        ( MkBinaryOp
            { binaryFn = (||)
            , binaryName = "or"
            , binarySymbol = Just "||"
            , binaryCommutative = True
            , binaryPrecedence = 2
            }
        )

(.^^) :: (Columnable a, Num a) => Expr a -> Int -> Expr a
(.^^) expr i =
    Binary
        ( MkBinaryOp
            { binaryFn = (^)
            , binaryName = "pow"
            , binarySymbol = Just "^"
            , binaryCommutative = False
            , binaryPrecedence = 8
            }
        )
        expr
        (Lit i)
