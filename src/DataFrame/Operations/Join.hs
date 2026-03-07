{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Join where

import Control.Applicative ((<|>))
import Control.Monad (forM_)
import Control.Monad.ST (ST, runST)
import qualified Data.HashMap.Strict as HM
import Data.List (foldl')
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe)
import Data.STRef (newSTRef, readSTRef, writeSTRef)
import qualified Data.Set as S
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (..))
import qualified Data.Vector as VB
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import DataFrame.Internal.Column as D
import DataFrame.Internal.DataFrame as D
import DataFrame.Operations.Aggregation as D
import DataFrame.Operations.Core as D
import Type.Reflection

-- | Equivalent to SQL join types.
data JoinType
    = INNER
    | LEFT
    | RIGHT
    | FULL_OUTER
    deriving (Show)

-- | Join two dataframes using SQL join semantics.
join ::
    JoinType ->
    [T.Text] ->
    DataFrame -> -- Right hand side
    DataFrame -> -- Left hand side
    DataFrame
join INNER xs right = innerJoin xs right
join LEFT xs right = leftJoin xs right
join RIGHT xs right = rightJoin xs right
join FULL_OUTER xs right = fullOuterJoin xs right

{- | Row-count threshold for the build side.
When the build side exceeds this, sort-merge join is used
instead of hash join to avoid L3 cache thrashing.
-}
joinStrategyThreshold :: Int
joinStrategyThreshold = 500_000

{- | A compact index mapping hash values to contiguous slices of
original row indices. All indices live in a single unboxed vector;
the HashMap stores @(offset, length)@ into that vector.
-}
data CompactIndex = CompactIndex
    { ciSortedIndices :: {-# UNPACK #-} !(VU.Vector Int)
    , ciOffsets :: !(HM.HashMap Int (Int, Int))
    }

{- | Build a compact index from a vector of row hashes.
Sorts @(hash, originalIndex)@ pairs by hash, then scans for
contiguous runs to populate the offset map.
-}
buildCompactIndex :: VU.Vector Int -> CompactIndex
buildCompactIndex hashes =
    let n = VU.length hashes
        sorted = runST $ do
            mv <- VU.thaw (VU.zip hashes (VU.enumFromN 0 n))
            VA.sortBy (\(h1, _) (h2, _) -> compare h1 h2) mv
            VU.unsafeFreeze mv
        sortedHashes = VU.map fst sorted
        sortedIndices = VU.map snd sorted
        !offsets = buildOffsets sortedHashes n 0 HM.empty
     in CompactIndex sortedIndices offsets
  where
    buildOffsets ::
        VU.Vector Int ->
        Int ->
        Int ->
        HM.HashMap Int (Int, Int) ->
        HM.HashMap Int (Int, Int)
    buildOffsets !sh !n !i !acc
        | i >= n = acc
        | otherwise =
            let !h = sh `VU.unsafeIndex` i
                !end = findGroupEnd sh h (i + 1) n
             in buildOffsets sh n end (HM.insert h (i, end - i) acc)

-- | Find the end of a contiguous run of equal values starting at @j@.
findGroupEnd :: VU.Vector Int -> Int -> Int -> Int -> Int
findGroupEnd !v !h !j !n
    | j >= n = j
    | v `VU.unsafeIndex` j == h = findGroupEnd v h (j + 1) n
    | otherwise = j
{-# INLINE findGroupEnd #-}

-- | Sort a hash vector, returning sorted hashes and corresponding original indices.
sortWithIndices :: VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
sortWithIndices hashes =
    let n = VU.length hashes
        sorted = runST $ do
            mv <- VU.thaw (VU.zip hashes (VU.enumFromN 0 n))
            VA.sortBy (\(h1, _) (h2, _) -> compare h1 h2) mv
            VU.unsafeFreeze mv
     in (VU.map fst sorted, VU.map snd sorted)

-- | Write the cross product of two index ranges into mutable vectors.
fillCrossProduct ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    Int ->
    Int ->
    Int ->
    VUM.MVector s Int ->
    VUM.MVector s Int ->
    Int ->
    ST s ()
fillCrossProduct !leftSI !rightSI !lStart !lEnd !rStart !rEnd !lv !rv !pos = goL lStart pos
  where
    !rLen = rEnd - rStart
    goL !li !p
        | li >= lEnd = return ()
        | otherwise = do
            let !lOrigIdx = leftSI `VU.unsafeIndex` li
            goR lOrigIdx rStart p
            goL (li + 1) (p + rLen)
    goR !lOrigIdx !ri !q
        | ri >= rEnd = return ()
        | otherwise = do
            VUM.unsafeWrite lv q lOrigIdx
            VUM.unsafeWrite rv q (rightSI `VU.unsafeIndex` ri)
            goR lOrigIdx (ri + 1) (q + 1)
{-# INLINE fillCrossProduct #-}

-- | Compute key-column indices from the column index map.
keyColIndices :: S.Set T.Text -> DataFrame -> [Int]
keyColIndices csSet df = M.elems $ M.restrictKeys (D.columnIndices df) csSet

-- ============================================================
-- Inner Join
-- ============================================================

{- | Performs an inner join on two dataframes using the specified key columns.
Returns only rows where the key values exist in both dataframes.

==== __Example__
@
ghci> df = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2", "K3"]), ("A", D.fromList ["A0", "A1", "A2", "A3"])]
ghci> other = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2"]), ("B", D.fromList ["B0", "B1", "B2"])]
ghci> D.innerJoin ["key"] df other

-----------------
 key  |  A  |  B
------|-----|----
 Text | Text| Text
------|-----|----
 K0   | A0  | B0
 K1   | A1  | B1
 K2   | A2  | B2

@
-}
innerJoin :: [T.Text] -> DataFrame -> DataFrame -> DataFrame
innerJoin cs left right
    | D.null right || D.null left = D.empty
    | otherwise =
        let
            csSet = S.fromList cs
            leftRows = fst (D.dimensions left)
            rightRows = fst (D.dimensions right)

            leftKeyIdxs = keyColIndices csSet left
            rightKeyIdxs = keyColIndices csSet right
            leftHashes = D.computeRowHashes leftKeyIdxs left
            rightHashes = D.computeRowHashes rightKeyIdxs right

            buildRows = min leftRows rightRows
            (leftIxs, rightIxs)
                | buildRows > joinStrategyThreshold =
                    sortMergeInnerKernel leftHashes rightHashes
                | rightRows <= leftRows =
                    -- Build on right (smaller or equal), probe with left
                    hashInnerKernel leftHashes rightHashes
                | otherwise =
                    -- Build on left (smaller), probe with right, swap result
                    let (!rIxs, !lIxs) = hashInnerKernel rightHashes leftHashes
                     in (lIxs, rIxs)
         in
            assembleInner csSet left right leftIxs rightIxs

{- | Hash-based inner join kernel.
Builds compact index on @buildHashes@ (second arg), probes with
@probeHashes@ (first arg).
Returns @(probeExpandedIndices, buildExpandedIndices)@.
-}
hashInnerKernel ::
    VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
hashInnerKernel probeHashes buildHashes = runST $ do
    let ci = buildCompactIndex buildHashes
        ciIxs = ciSortedIndices ci
        ciOff = ciOffsets ci
        !probeN = VU.length probeHashes

    -- Pass 1: count total output rows
    let !totalRows =
            VU.foldl'
                ( \acc h ->
                    case HM.lookup h ciOff of
                        Nothing -> acc
                        Just (_, len) -> acc + len
                )
                0
                probeHashes

    -- Pass 2: fill output vectors
    pv <- VUM.unsafeNew totalRows
    bv <- VUM.unsafeNew totalRows
    posRef <- newSTRef (0 :: Int)

    let go !i
            | i >= probeN = return ()
            | otherwise = do
                let !h = probeHashes `VU.unsafeIndex` i
                case HM.lookup h ciOff of
                    Nothing -> go (i + 1)
                    Just (!start, !len) -> do
                        !p <- readSTRef posRef
                        fillBuild i start len p 0
                        writeSTRef posRef (p + len)
                        go (i + 1)
        fillBuild !probeIdx !start !len !p !j
            | j >= len = return ()
            | otherwise = do
                VUM.unsafeWrite pv (p + j) probeIdx
                VUM.unsafeWrite bv (p + j) (ciIxs `VU.unsafeIndex` (start + j))
                fillBuild probeIdx start len p (j + 1)
    go 0

    (,) <$> VU.unsafeFreeze pv <*> VU.unsafeFreeze bv

{- | Sort-merge inner join kernel.
Sorts both sides by hash, walks in lockstep.
Returns @(leftExpandedIndices, rightExpandedIndices)@.
-}
sortMergeInnerKernel ::
    VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
sortMergeInnerKernel leftHashes rightHashes = runST $ do
    let (leftSH, leftSI) = sortWithIndices leftHashes
        (rightSH, rightSI) = sortWithIndices rightHashes
        !leftN = VU.length leftHashes
        !rightN = VU.length rightHashes

    -- Pass 1: count
    let countLoop !li !ri !c
            | li >= leftN || ri >= rightN = c
            | lh < rh = countLoop (li + 1) ri c
            | lh > rh = countLoop li (ri + 1) c
            | otherwise =
                let !lEnd = findGroupEnd leftSH lh (li + 1) leftN
                    !rEnd = findGroupEnd rightSH rh (ri + 1) rightN
                 in countLoop lEnd rEnd (c + (lEnd - li) * (rEnd - ri))
          where
            !lh = leftSH `VU.unsafeIndex` li
            !rh = rightSH `VU.unsafeIndex` ri
        !totalRows = countLoop 0 0 0

    -- Pass 2: fill
    lv <- VUM.unsafeNew totalRows
    rv <- VUM.unsafeNew totalRows

    let fill !li !ri !pos
            | li >= leftN || ri >= rightN = return ()
            | lh < rh = fill (li + 1) ri pos
            | lh > rh = fill li (ri + 1) pos
            | otherwise = do
                let !lEnd = findGroupEnd leftSH lh (li + 1) leftN
                    !rEnd = findGroupEnd rightSH rh (ri + 1) rightN
                    !groupSize = (lEnd - li) * (rEnd - ri)
                fillCrossProduct leftSI rightSI li lEnd ri rEnd lv rv pos
                fill lEnd rEnd (pos + groupSize)
          where
            !lh = leftSH `VU.unsafeIndex` li
            !rh = rightSH `VU.unsafeIndex` ri

    fill 0 0 0
    (,) <$> VU.unsafeFreeze lv <*> VU.unsafeFreeze rv

-- | Assemble the result DataFrame for an inner join from expanded index vectors.
assembleInner ::
    S.Set T.Text ->
    DataFrame ->
    DataFrame ->
    VU.Vector Int ->
    VU.Vector Int ->
    DataFrame
assembleInner csSet left right leftIxs rightIxs =
    let !resultLen = VU.length leftIxs
        leftColSet = S.fromList (D.columnNames left)
        rightColNames = D.columnNames right

        -- Pre-expand every column once
        expandedLeftCols = VB.map (D.atIndicesStable leftIxs) (D.columns left)
        expandedRightCols = VB.map (D.atIndicesStable rightIxs) (D.columns right)

        getExpandedLeft name = do
            idx <- M.lookup name (D.columnIndices left)
            return (expandedLeftCols `VB.unsafeIndex` idx)

        getExpandedRight name = do
            idx <- M.lookup name (D.columnIndices right)
            return (expandedRightCols `VB.unsafeIndex` idx)

        -- Base DataFrame: all left columns, expanded
        baseDf =
            left
                { columns = expandedLeftCols
                , dataframeDimensions = (resultLen, snd (D.dataframeDimensions left))
                , derivingExpressions = M.empty
                }

        insertIfPresent _ Nothing df = df
        insertIfPresent name (Just c) df = D.insertColumn name c df
     in D.fold
            ( \name df ->
                if S.member name csSet
                    then df -- Key column already present from left side
                    else
                        if S.member name leftColSet
                            then -- Overlapping non-key column: merge with These
                                insertIfPresent
                                    name
                                    (D.mergeColumns <$> getExpandedLeft name <*> getExpandedRight name)
                                    df
                            else -- Right-only column
                                insertIfPresent name (getExpandedRight name) df
            )
            rightColNames
            baseDf

-- ============================================================
-- Left Join
-- ============================================================

{- | Performs a left join on two dataframes using the specified key columns.
Returns all rows from the left dataframe, with matching rows from the right dataframe.
Non-matching rows will have Nothing/null values for columns from the right dataframe.

==== __Example__
@
ghci> df = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2", "K3"]), ("A", D.fromList ["A0", "A1", "A2", "A3"])]
ghci> other = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2"]), ("B", D.fromList ["B0", "B1", "B2"])]
ghci> D.leftJoin ["key"] df other

------------------------
 key  |  A  |     B
------|-----|----------
 Text | Text| Maybe Text
------|-----|----------
 K0   | A0  | Just "B0"
 K1   | A1  | Just "B1"
 K2   | A2  | Just "B2"
 K3   | A3  | Nothing

@
-}
leftJoin :: [T.Text] -> DataFrame -> DataFrame -> DataFrame
leftJoin cs left right
    | D.null right || D.nRows right == 0 = left
    | D.null left || D.nRows left == 0 = D.empty
    | otherwise =
        let
            csSet = S.fromList cs
            rightRows = fst (D.dimensions right)

            leftKeyIdxs = keyColIndices csSet left
            rightKeyIdxs = keyColIndices csSet right
            leftHashes = D.computeRowHashes leftKeyIdxs left
            rightHashes = D.computeRowHashes rightKeyIdxs right

            -- Right is always the build side for left join
            (leftIxs, rightIxs)
                | rightRows > joinStrategyThreshold =
                    sortMergeLeftKernel leftHashes rightHashes
                | otherwise =
                    hashLeftKernel leftHashes rightHashes
         in
            -- rightIxs uses -1 as sentinel for "no match"
            assembleLeft csSet left right leftIxs rightIxs

{- | Hash-based left join kernel.
Returns @(leftExpandedIndices, rightExpandedIndices)@ where
right indices use @-1@ as sentinel for unmatched rows.
-}
hashLeftKernel ::
    VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
hashLeftKernel leftHashes rightHashes = runST $ do
    let ci = buildCompactIndex rightHashes
        ciIxs = ciSortedIndices ci
        ciOff = ciOffsets ci
        !leftN = VU.length leftHashes

    -- Pass 1: count
    let !totalRows =
            VU.foldl'
                ( \acc h ->
                    case HM.lookup h ciOff of
                        Nothing -> acc + 1 -- Unmatched left row still produces one output row
                        Just (_, len) -> acc + len
                )
                0
                leftHashes

    -- Pass 2: fill
    lv <- VUM.unsafeNew totalRows
    rv <- VUM.unsafeNew totalRows
    posRef <- newSTRef (0 :: Int)

    let go !i
            | i >= leftN = return ()
            | otherwise = do
                let !h = leftHashes `VU.unsafeIndex` i
                !p <- readSTRef posRef
                case HM.lookup h ciOff of
                    Nothing -> do
                        VUM.unsafeWrite lv p i
                        VUM.unsafeWrite rv p (-1)
                        writeSTRef posRef (p + 1)
                    Just (!start, !len) -> do
                        fillBuild i start len p 0
                        writeSTRef posRef (p + len)
                go (i + 1)
        fillBuild !leftIdx !start !len !p !j
            | j >= len = return ()
            | otherwise = do
                VUM.unsafeWrite lv (p + j) leftIdx
                VUM.unsafeWrite rv (p + j) (ciIxs `VU.unsafeIndex` (start + j))
                fillBuild leftIdx start len p (j + 1)
    go 0

    (,) <$> VU.unsafeFreeze lv <*> VU.unsafeFreeze rv

{- | Sort-merge left join kernel.
Returns @(leftExpandedIndices, rightExpandedIndices)@ with @-1@ sentinel.
-}
sortMergeLeftKernel ::
    VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
sortMergeLeftKernel leftHashes rightHashes = runST $ do
    let (leftSH, leftSI) = sortWithIndices leftHashes
        (rightSH, rightSI) = sortWithIndices rightHashes
        !leftN = VU.length leftHashes
        !rightN = VU.length rightHashes

    -- Pass 1: count
    let countLoop !li !ri !c
            | li >= leftN = c
            | ri >= rightN = c + (leftN - li)
            | lh < rh = countLoop (li + 1) ri (c + 1)
            | lh > rh = countLoop li (ri + 1) c
            | otherwise =
                let !lEnd = findGroupEnd leftSH lh (li + 1) leftN
                    !rEnd = findGroupEnd rightSH rh (ri + 1) rightN
                 in countLoop lEnd rEnd (c + (lEnd - li) * (rEnd - ri))
          where
            !lh = leftSH `VU.unsafeIndex` li
            !rh = rightSH `VU.unsafeIndex` ri
        !totalRows = countLoop 0 0 0

    -- Pass 2: fill
    lv <- VUM.unsafeNew totalRows
    rv <- VUM.unsafeNew totalRows

    let fill !li !ri !pos
            | li >= leftN = return ()
            | ri >= rightN = fillRemainingLeft li pos
            | lh < rh = do
                VUM.unsafeWrite lv pos (leftSI `VU.unsafeIndex` li)
                VUM.unsafeWrite rv pos (-1)
                fill (li + 1) ri (pos + 1)
            | lh > rh = fill li (ri + 1) pos
            | otherwise = do
                let !lEnd = findGroupEnd leftSH lh (li + 1) leftN
                    !rEnd = findGroupEnd rightSH rh (ri + 1) rightN
                    !groupSize = (lEnd - li) * (rEnd - ri)
                fillCrossProduct leftSI rightSI li lEnd ri rEnd lv rv pos
                fill lEnd rEnd (pos + groupSize)
          where
            !lh = leftSH `VU.unsafeIndex` li
            !rh = rightSH `VU.unsafeIndex` ri

        fillRemainingLeft !i !pos
            | i >= leftN = return ()
            | otherwise = do
                VUM.unsafeWrite lv pos (leftSI `VU.unsafeIndex` i)
                VUM.unsafeWrite rv pos (-1)
                fillRemainingLeft (i + 1) (pos + 1)

    fill 0 0 0
    (,) <$> VU.unsafeFreeze lv <*> VU.unsafeFreeze rv

{- | Assemble the result DataFrame for a left join.
Right index vectors use @-1@ sentinel, gathered via 'gatherWithSentinel'.
-}
assembleLeft ::
    S.Set T.Text ->
    DataFrame ->
    DataFrame ->
    VU.Vector Int ->
    VU.Vector Int ->
    DataFrame
assembleLeft csSet left right leftIxs rightIxs =
    let !resultLen = VU.length leftIxs
        leftColSet = S.fromList (D.columnNames left)
        rightColNames = D.columnNames right

        expandedLeftCols = VB.map (D.atIndicesStable leftIxs) (D.columns left)
        expandedRightCols = VB.map (D.gatherWithSentinel rightIxs) (D.columns right)

        getExpandedLeft name = do
            idx <- M.lookup name (D.columnIndices left)
            return (expandedLeftCols `VB.unsafeIndex` idx)

        getExpandedRight name = do
            idx <- M.lookup name (D.columnIndices right)
            return (expandedRightCols `VB.unsafeIndex` idx)

        baseDf =
            left
                { columns = expandedLeftCols
                , dataframeDimensions = (resultLen, snd (D.dataframeDimensions left))
                , derivingExpressions = M.empty
                }

        insertIfPresent _ Nothing df = df
        insertIfPresent name (Just c) df = D.insertColumn name c df
     in D.fold
            ( \name df ->
                if S.member name csSet
                    then df
                    else
                        if S.member name leftColSet
                            then
                                insertIfPresent
                                    name
                                    (D.mergeColumns <$> getExpandedLeft name <*> getExpandedRight name)
                                    df
                            else insertIfPresent name (getExpandedRight name) df
            )
            rightColNames
            baseDf

{- | Performs a right join on two dataframes using the specified key columns.
Returns all rows from the right dataframe, with matching rows from the left dataframe.
Non-matching rows will have Nothing/null values for columns from the left dataframe.

==== __Example__
@
ghci> df = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2", "K3"]), ("A", D.fromList ["A0", "A1", "A2", "A3"])]
ghci> other = D.fromNamedColumns [("key", D.fromList ["K0", "K1"]), ("B", D.fromList ["B0", "B1"])]
ghci> D.rightJoin ["key"] df other

-----------------
 key  |  A  |  B
------|-----|----
 Text | Text| Text
------|-----|----
 K0   | A0  | B0
 K1   | A1  | B1

@
-}
rightJoin ::
    [T.Text] -> DataFrame -> DataFrame -> DataFrame
rightJoin cs left right = leftJoin cs right left

fullOuterJoin ::
    [T.Text] -> DataFrame -> DataFrame -> DataFrame
fullOuterJoin cs left right
    | D.null right || D.nRows right == 0 = left
    | D.null left || D.nRows left == 0 = right
    | otherwise =
        let
            csSet = S.fromList cs
            leftRows = fst (D.dimensions left)
            rightRows = fst (D.dimensions right)

            leftKeyIdxs = keyColIndices csSet left
            rightKeyIdxs = keyColIndices csSet right
            leftHashes = D.computeRowHashes leftKeyIdxs left
            rightHashes = D.computeRowHashes rightKeyIdxs right

            -- Both sides can have nulls in full outer
            (leftIxs, rightIxs)
                | max leftRows rightRows > joinStrategyThreshold =
                    sortMergeFullOuterKernel leftHashes rightHashes
                | otherwise =
                    hashFullOuterKernel leftHashes rightHashes
         in
            -- Both index vectors use -1 as sentinel
            assembleFullOuter csSet left right leftIxs rightIxs

{- | Hash-based full outer join kernel.
Builds compact indices on both sides.
Returns @(leftExpandedIndices, rightExpandedIndices)@ with @-1@ sentinels.
-}
hashFullOuterKernel ::
    VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
hashFullOuterKernel leftHashes rightHashes = runST $ do
    let leftCI = buildCompactIndex leftHashes
        rightCI = buildCompactIndex rightHashes
        leftOff = ciOffsets leftCI
        rightOff = ciOffsets rightCI
        leftSI = ciSortedIndices leftCI
        rightSI = ciSortedIndices rightCI

    -- Count: matched + left-only + right-only
    let leftEntries = HM.toList leftOff
        rightEntries = HM.toList rightOff

        !matchedCount =
            foldl'
                ( \acc (h, (_, ll)) ->
                    case HM.lookup h rightOff of
                        Nothing -> acc
                        Just (_, rl) -> acc + ll * rl
                )
                0
                leftEntries

        !leftOnlyCount =
            foldl'
                ( \acc (h, (_, ll)) ->
                    if HM.member h rightOff then acc else acc + ll
                )
                0
                leftEntries

        !rightOnlyCount =
            foldl'
                ( \acc (h, (_, rl)) ->
                    if HM.member h leftOff then acc else acc + rl
                )
                0
                rightEntries

        !totalCount = matchedCount + leftOnlyCount + rightOnlyCount

    lv <- VUM.unsafeNew totalCount
    rv <- VUM.unsafeNew totalCount
    posRef <- newSTRef (0 :: Int)

    -- Fill matched + left-only (iterate left keys)
    forM_ leftEntries $ \(h, (lStart, lLen)) -> do
        !p <- readSTRef posRef
        case HM.lookup h rightOff of
            Nothing -> do
                -- Left-only rows
                let goL !j !q
                        | j >= lLen = return ()
                        | otherwise = do
                            VUM.unsafeWrite lv q (leftSI `VU.unsafeIndex` (lStart + j))
                            VUM.unsafeWrite rv q (-1)
                            goL (j + 1) (q + 1)
                goL 0 p
                writeSTRef posRef (p + lLen)
            Just (!rStart, !rLen) -> do
                -- Cross product
                fillCrossProduct
                    leftSI
                    rightSI
                    lStart
                    (lStart + lLen)
                    rStart
                    (rStart + rLen)
                    lv
                    rv
                    p
                writeSTRef posRef (p + lLen * rLen)

    -- Fill right-only (iterate right keys not in left)
    forM_ rightEntries $ \(h, (rStart, rLen)) ->
        case HM.lookup h leftOff of
            Just _ -> return ()
            Nothing -> do
                !p <- readSTRef posRef
                let goR !j !q
                        | j >= rLen = return ()
                        | otherwise = do
                            VUM.unsafeWrite lv q (-1)
                            VUM.unsafeWrite rv q (rightSI `VU.unsafeIndex` (rStart + j))
                            goR (j + 1) (q + 1)
                goR 0 p
                writeSTRef posRef (p + rLen)

    (,) <$> VU.unsafeFreeze lv <*> VU.unsafeFreeze rv

{- | Sort-merge full outer join kernel.
Returns @(leftExpandedIndices, rightExpandedIndices)@ with @-1@ sentinels.
-}
sortMergeFullOuterKernel ::
    VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
sortMergeFullOuterKernel leftHashes rightHashes = runST $ do
    let (leftSH, leftSI) = sortWithIndices leftHashes
        (rightSH, rightSI) = sortWithIndices rightHashes
        !leftN = VU.length leftHashes
        !rightN = VU.length rightHashes

    -- Pass 1: count
    let countLoop !li !ri !c
            | li >= leftN && ri >= rightN = c
            | li >= leftN = c + (rightN - ri)
            | ri >= rightN = c + (leftN - li)
            | lh < rh = countLoop (li + 1) ri (c + 1)
            | lh > rh = countLoop li (ri + 1) (c + 1)
            | otherwise =
                let !lEnd = findGroupEnd leftSH lh (li + 1) leftN
                    !rEnd = findGroupEnd rightSH rh (ri + 1) rightN
                 in countLoop lEnd rEnd (c + (lEnd - li) * (rEnd - ri))
          where
            !lh = leftSH `VU.unsafeIndex` li
            !rh = rightSH `VU.unsafeIndex` ri
        !totalRows = countLoop 0 0 0

    -- Pass 2: fill
    lv <- VUM.unsafeNew totalRows
    rv <- VUM.unsafeNew totalRows

    let fill !li !ri !pos
            | li >= leftN && ri >= rightN = return ()
            | li >= leftN = fillRemainingRight ri pos
            | ri >= rightN = fillRemainingLeft li pos
            | lh < rh = do
                VUM.unsafeWrite lv pos (leftSI `VU.unsafeIndex` li)
                VUM.unsafeWrite rv pos (-1)
                fill (li + 1) ri (pos + 1)
            | lh > rh = do
                VUM.unsafeWrite lv pos (-1)
                VUM.unsafeWrite rv pos (rightSI `VU.unsafeIndex` ri)
                fill li (ri + 1) (pos + 1)
            | otherwise = do
                let !lEnd = findGroupEnd leftSH lh (li + 1) leftN
                    !rEnd = findGroupEnd rightSH rh (ri + 1) rightN
                    !groupSize = (lEnd - li) * (rEnd - ri)
                fillCrossProduct leftSI rightSI li lEnd ri rEnd lv rv pos
                fill lEnd rEnd (pos + groupSize)
          where
            !lh = leftSH `VU.unsafeIndex` li
            !rh = rightSH `VU.unsafeIndex` ri

        fillRemainingLeft !i !pos
            | i >= leftN = return ()
            | otherwise = do
                VUM.unsafeWrite lv pos (leftSI `VU.unsafeIndex` i)
                VUM.unsafeWrite rv pos (-1)
                fillRemainingLeft (i + 1) (pos + 1)

        fillRemainingRight !i !pos
            | i >= rightN = return ()
            | otherwise = do
                VUM.unsafeWrite lv pos (-1)
                VUM.unsafeWrite rv pos (rightSI `VU.unsafeIndex` i)
                fillRemainingRight (i + 1) (pos + 1)

    fill 0 0 0
    (,) <$> VU.unsafeFreeze lv <*> VU.unsafeFreeze rv

{- | Assemble the result DataFrame for a full outer join.
Both index vectors use @-1@ sentinel; all columns gathered via
'gatherWithSentinel'.  Key columns are coalesced (first non-null wins).
-}
assembleFullOuter ::
    S.Set T.Text ->
    DataFrame ->
    DataFrame ->
    VU.Vector Int ->
    VU.Vector Int ->
    DataFrame
assembleFullOuter csSet left right leftIxs rightIxs =
    let !resultLen = VU.length leftIxs
        leftColSet = S.fromList (D.columnNames left)
        rightColNames = D.columnNames right

        expandedLeftCols = VB.map (D.gatherWithSentinel leftIxs) (D.columns left)
        expandedRightCols = VB.map (D.gatherWithSentinel rightIxs) (D.columns right)

        getExpandedLeft name = do
            idx <- M.lookup name (D.columnIndices left)
            return (expandedLeftCols `VB.unsafeIndex` idx)

        getExpandedRight name = do
            idx <- M.lookup name (D.columnIndices right)
            return (expandedRightCols `VB.unsafeIndex` idx)

        baseDf =
            left
                { columns = expandedLeftCols
                , dataframeDimensions = (resultLen, snd (D.dataframeDimensions left))
                , derivingExpressions = M.empty
                }

        insertIfPresent _ Nothing df = df
        insertIfPresent name (Just c) df = D.insertColumn name c df

        -- Coalesce two OptionalColumns: take first non-Nothing per row,
        -- producing a non-optional column.
        coalesceKeyColumn :: Column -> Column -> Column
        coalesceKeyColumn
            (OptionalColumn (lCol :: VB.Vector (Maybe a)))
            (OptionalColumn (rCol :: VB.Vector (Maybe b))) =
                case testEquality (typeRep @a) (typeRep @b) of
                    Just Refl ->
                        D.fromVector $
                            VB.zipWith
                                ( \l r ->
                                    fromMaybe (error "fullOuterJoin: null on both sides of key column") (l <|> r)
                                )
                                lCol
                                rCol
                    Nothing -> error "Cannot join columns of different types"
        coalesceKeyColumn _ _ = error "fullOuterJoin: expected OptionalColumn for key columns"
     in D.fold
            ( \name df ->
                if S.member name csSet
                    then -- Key column: coalesce left and right
                        case (getExpandedLeft name, getExpandedRight name) of
                            (Just lc, Just rc) -> D.insertColumn name (coalesceKeyColumn lc rc) df
                            _ -> df
                    else
                        if S.member name leftColSet
                            then
                                insertIfPresent
                                    name
                                    (D.mergeColumns <$> getExpandedLeft name <*> getExpandedRight name)
                                    df
                            else insertIfPresent name (getExpandedRight name) df
            )
            rightColNames
            baseDf
