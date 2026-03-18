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
    NullLift2Op (applyNull2),
    NullableCmpOp (nullCmpOp),
    NumericWidenOp,
    WidenResult,
    widenArithOp,
 )
import DataFrame.Internal.Types (Promote)

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
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (Promote (BaseType a) (BaseType b)) (WidenResult a b)
    , Num (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (WidenResult a b)
(.+) =
    Binary
        ( MkBinaryOp
            { binaryFn = applyNull2 (widenArithOp (+))
            , binaryName = "nulladd"
            , binarySymbol = Just "+"
            , binaryCommutative = True
            , binaryPrecedence = 6
            }
        )

-- | Nullable-aware subtraction.
(.-) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (Promote (BaseType a) (BaseType b)) (WidenResult a b)
    , Num (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (WidenResult a b)
(.-) =
    Binary
        ( MkBinaryOp
            { binaryFn = applyNull2 (widenArithOp (-))
            , binaryName = "nullsub"
            , binarySymbol = Just "-"
            , binaryCommutative = False
            , binaryPrecedence = 6
            }
        )

-- | Nullable-aware multiplication.
(.*) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (Promote (BaseType a) (BaseType b)) (WidenResult a b)
    , Num (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (WidenResult a b)
(.*) =
    Binary
        ( MkBinaryOp
            { binaryFn = applyNull2 (widenArithOp (*))
            , binaryName = "nullmul"
            , binarySymbol = Just "*"
            , binaryCommutative = True
            , binaryPrecedence = 7
            }
        )

-- | Nullable-aware division.
(./) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (Promote (BaseType a) (BaseType b)) (WidenResult a b)
    , Fractional (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (WidenResult a b)
(./) =
    Binary
        ( MkBinaryOp
            { binaryFn = applyNull2 (widenArithOp (/))
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

(.^^) ::
    ( Columnable (BaseType a)
    , Columnable (BaseType b)
    , Fractional (BaseType a)
    , Integral (BaseType b)
    , NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (BaseType a) a
    , Num (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a -> Expr b -> Expr a
(.^^) =
    Binary
        ( MkBinaryOp
            { binaryFn = applyNull2 (^^)
            , binaryName = "pow"
            , binarySymbol = Just "^^"
            , binaryCommutative = False
            , binaryPrecedence = 8
            }
        )

(.^) ::
    ( Columnable (BaseType a)
    , Columnable (BaseType b)
    , Num (BaseType a)
    , Integral (BaseType b)
    , NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (BaseType a) a
    , Num (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a -> Expr b -> Expr a
(.^) =
    Binary
        ( MkBinaryOp
            { binaryFn = applyNull2 (^)
            , binaryName = "pow"
            , binarySymbol = Just "^^"
            , binaryCommutative = False
            , binaryPrecedence = 8
            }
        )
