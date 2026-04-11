{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Aggregation where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Radix as VA
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Exception (throw)
import Control.Monad
import Control.Monad.ST (runST)
import Data.Bits
import Data.Hashable
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Errors
import DataFrame.Internal.Column (
    Column (..),
    TypedColumn (..),
    atIndicesStable,
    bitmapTestBit,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), GroupedDataFrame (..))
import DataFrame.Internal.Expression
import DataFrame.Internal.Interpreter
import DataFrame.Internal.Types
import DataFrame.Operations.Core
import DataFrame.Operations.Subset
import Type.Reflection (typeRep)

{- | O(k * n) groups the dataframe by the given rows aggregating the remaining rows
into vector that should be reduced later.
-}
groupBy ::
    [T.Text] ->
    DataFrame ->
    GroupedDataFrame
groupBy names df
    | any (`notElem` columnNames df) names =
        throw $
            ColumnsNotFoundException
                (names L.\\ columnNames df)
                "groupBy"
                (columnNames df)
    | nRows df == 0 =
        Grouped
            df
            names
            VU.empty
            (VU.fromList [0])
            VU.empty
    | otherwise =
        let !vis = VU.map fst valIndices
            !os = changingPoints valIndices
            !n = fst (dimensions df)
         in Grouped
                df
                names
                vis
                os
                (buildRowToGroup n vis os)
  where
    indicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` names) (columnIndices df)
    doubleToInt :: Double -> Int
    doubleToInt = floor . (* 1000)
    valIndices = runST $ do
        let n = fst (dimensions df)
        mv <- VUM.new n

        let selectedCols = map (columns df V.!) indicesToGroup

        forM_ selectedCols $ \case
            UnboxedColumn _ (v :: VU.Vector a) ->
                case testEquality (typeRep @a) (typeRep @Int) of
                    Just Refl ->
                        VU.imapM_
                            ( \i x -> do
                                (_, !h) <- VUM.unsafeRead mv i
                                VUM.unsafeWrite mv i (i, hashWithSalt h x)
                            )
                            v
                    Nothing ->
                        case testEquality (typeRep @a) (typeRep @Double) of
                            Just Refl ->
                                VU.imapM_
                                    ( \i d -> do
                                        (_, !h) <- VUM.unsafeRead mv i
                                        VUM.unsafeWrite mv i (i, hashWithSalt h (doubleToInt d))
                                    )
                                    v
                            Nothing ->
                                case sIntegral @a of
                                    STrue ->
                                        VU.imapM_
                                            ( \i d -> do
                                                let x :: Int
                                                    x = fromIntegral @a @Int d
                                                (_, !h) <- VUM.unsafeRead mv i
                                                VUM.unsafeWrite mv i (i, hashWithSalt h x)
                                            )
                                            v
                                    SFalse ->
                                        case sFloating @a of
                                            STrue ->
                                                VU.imapM_
                                                    ( \i d -> do
                                                        let x :: Int
                                                            x = doubleToInt (realToFrac d :: Double)
                                                        (_, !h) <- VUM.unsafeRead mv i
                                                        VUM.unsafeWrite mv i (i, hashWithSalt h x)
                                                    )
                                                    v
                                            SFalse ->
                                                VU.imapM_
                                                    ( \i d -> do
                                                        let x = hash (show d)
                                                        (_, !h) <- VUM.unsafeRead mv i
                                                        VUM.unsafeWrite mv i (i, hashWithSalt h x)
                                                    )
                                                    v
            BoxedColumn bm (v :: V.Vector a) ->
                case testEquality (typeRep @a) (typeRep @T.Text) of
                    Just Refl ->
                        V.imapM_
                            ( \i t -> do
                                (_, !h) <- VUM.unsafeRead mv i
                                let h' = case bm of
                                        Just bm' | not (bitmapTestBit bm' i) -> hashWithSalt h (0 :: Int) -- null sentinel
                                        _ -> hashWithSalt h t
                                VUM.unsafeWrite mv i (i, h')
                            )
                            v
                    Nothing ->
                        V.imapM_
                            ( \i d -> do
                                (_, !h) <- VUM.unsafeRead mv i
                                let h' = case bm of
                                        Just bm' | not (bitmapTestBit bm' i) -> hashWithSalt h (0 :: Int) -- null sentinel
                                        _ -> hashWithSalt h (hash (show d))
                                VUM.unsafeWrite mv i (i, h')
                            )
                            v

        let numPasses = 4
            bucketSize = 65536
            radixFunc k (_, !h) =
                let h' = fromIntegral h `xor` (1 `unsafeShiftL` 63) :: Word
                    shiftBits = k * 16
                 in fromIntegral ((h' `unsafeShiftR` shiftBits) .&. 65535)
        VA.sortBy numPasses bucketSize radixFunc mv
        VU.unsafeFreeze mv

{- | Build the rowToGroup lookup vector from valueIndices and offsets.
rowToGroup[i] = k means row i belongs to group k.
-}
buildRowToGroup :: Int -> VU.Vector Int -> VU.Vector Int -> VU.Vector Int
buildRowToGroup n vis os = runST $ do
    rtg <- VUM.new n
    let nGroups = VU.length os - 1
    forM_ [0 .. nGroups - 1] $ \k ->
        let s = VU.unsafeIndex os k
            e = VU.unsafeIndex os (k + 1)
         in forM_ [s .. e - 1] $ \i ->
                VUM.unsafeWrite rtg (VU.unsafeIndex vis i) k
    VU.unsafeFreeze rtg
{-# NOINLINE buildRowToGroup #-}

changingPoints :: VU.Vector (Int, Int) -> VU.Vector Int
changingPoints vs =
    VU.reverse
        (VU.fromList (VU.length vs : fst (VU.ifoldl' findChangePoints initialState vs)))
  where
    initialState = ([0], snd (VU.head vs))
    findChangePoints (!offs, !currentVal) index (_, !newVal)
        | currentVal == newVal = (offs, currentVal)
        | otherwise = (index : offs, newVal)

computeRowHashes :: [Int] -> DataFrame -> VU.Vector Int
computeRowHashes indices df = runST $ do
    let n = fst (dimensions df)
    mv <- VUM.new n

    let selectedCols = map (columns df V.!) indices

    forM_ selectedCols $ \case
        UnboxedColumn _ (v :: VU.Vector a) ->
            case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl ->
                    VU.imapM_
                        ( \i (x :: Int) -> do
                            h <- VUM.unsafeRead mv i
                            VUM.unsafeWrite mv i (hashWithSalt h x)
                        )
                        v
                Nothing ->
                    case testEquality (typeRep @a) (typeRep @Double) of
                        Just Refl ->
                            VU.imapM_
                                ( \i (d :: Double) -> do
                                    h <- VUM.unsafeRead mv i
                                    VUM.unsafeWrite mv i (hashWithSalt h (doubleToInt d))
                                )
                                v
                        Nothing ->
                            case sIntegral @a of
                                STrue ->
                                    VU.imapM_
                                        ( \i d -> do
                                            let x :: Int
                                                x = fromIntegral @a @Int d
                                            h <- VUM.unsafeRead mv i
                                            VUM.unsafeWrite mv i (hashWithSalt h x)
                                        )
                                        v
                                SFalse ->
                                    case sFloating @a of
                                        STrue ->
                                            VU.imapM_
                                                ( \i d -> do
                                                    let x :: Int
                                                        x = doubleToInt (realToFrac d :: Double)
                                                    h <- VUM.unsafeRead mv i
                                                    VUM.unsafeWrite mv i (hashWithSalt h x)
                                                )
                                                v
                                        SFalse ->
                                            VU.imapM_
                                                ( \i d -> do
                                                    let x = hash (show d)
                                                    h <- VUM.unsafeRead mv i
                                                    VUM.unsafeWrite mv i (hashWithSalt h x)
                                                )
                                                v
        BoxedColumn bm (v :: V.Vector a) ->
            case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl ->
                    V.imapM_
                        ( \i (t :: T.Text) -> do
                            h <- VUM.unsafeRead mv i
                            let h' = case bm of
                                    Just bm' | not (bitmapTestBit bm' i) -> hashWithSalt h (0 :: Int)
                                    _ -> hashWithSalt h t
                            VUM.unsafeWrite mv i h'
                        )
                        v
                Nothing ->
                    V.imapM_
                        ( \i d -> do
                            let x = case bm of
                                    Just bm' | not (bitmapTestBit bm' i) -> 0 :: Int
                                    _ -> hash (show d)
                            h <- VUM.unsafeRead mv i
                            VUM.unsafeWrite mv i (hashWithSalt h x)
                        )
                        v

    VU.unsafeFreeze mv
  where
    doubleToInt :: Double -> Int
    doubleToInt = floor . (* 1000)

{- | Aggregate a grouped dataframe using the expressions given.
All ungrouped columns will be dropped.
-}
aggregate :: [NamedExpr] -> GroupedDataFrame -> DataFrame
aggregate aggs gdf@(Grouped df groupingColumns valIndices offs _rowToGroup) =
    let
        df' =
            selectIndices
                (VU.map (valIndices VU.!) (VU.init offs))
                (select groupingColumns df)

        f (name, UExpr (expr :: Expr a)) d =
            let
                value = case interpretAggregation @a gdf expr of
                    Left e -> throw e
                    Right (UnAggregated _) -> throw $ UnaggregatedException (T.pack $ show expr)
                    Right (Aggregated (TColumn col)) -> col
             in
                insertColumn name value d
     in
        fold f aggs df'

selectIndices :: VU.Vector Int -> DataFrame -> DataFrame
selectIndices xs df =
    df
        { columns = V.map (atIndicesStable xs) (columns df)
        , dataframeDimensions = (VU.length xs, V.length (columns df))
        }

-- | Filter out all non-unique values in a dataframe.
distinct :: DataFrame -> DataFrame
distinct df = selectIndices (VU.map (indices VU.!) (VU.init os)) df
  where
    (Grouped _ _ indices os _rtg) = groupBy (columnNames df) df
