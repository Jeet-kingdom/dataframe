{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Internal.DataFrame where

import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Control.DeepSeq (NFData (..), rnf)
import Control.Exception (throw)
import Data.Function (on)
import Data.List (sortBy, transpose, (\\))
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import DataFrame.Display.Terminal.PrettyPrint
import DataFrame.Errors
import DataFrame.Internal.Column
import DataFrame.Internal.Expression
import Text.Printf
import Type.Reflection (Typeable, typeRep)
import Prelude hiding (null)

data DataFrame = DataFrame
    { columns :: V.Vector Column
    {- ^ Our main data structure stores a dataframe as
    a vector of columns. This improv
    -}
    , columnIndices :: M.Map T.Text Int
    -- ^ Keeps the column names in the order they were inserted in.
    , dataframeDimensions :: (Int, Int)
    -- ^ (rows, columns)
    , derivingExpressions :: M.Map T.Text UExpr
    }

instance NFData DataFrame where
    rnf (DataFrame cols idx dims _exprs) =
        rnf cols `seq` rnf idx `seq` rnf dims

{- | A record that contains information about how and what
rows are grouped in the dataframe. This can only be used with
`aggregate`.
-}
data GroupedDataFrame = Grouped
    { fullDataframe :: DataFrame
    , groupedColumns :: [T.Text]
    , valueIndices :: VU.Vector Int
    , offsets :: VU.Vector Int
    , rowToGroup :: VU.Vector Int
    {- ^ rowToGroup[i] = group index for row i.  Length n (one per row).
    Built once in 'groupBy'; reused by every aggregation.
    -}
    }

instance Show GroupedDataFrame where
    show (Grouped df cols _indices _os _rtg) =
        printf
            "{ keyColumns: %s groupedColumns: %s }"
            (show cols)
            (show (M.keys (columnIndices df) \\ cols))

instance Eq GroupedDataFrame where
    (==) (Grouped df cols _indices _os _rtg) (Grouped df' cols' _indices' _os' _rtg') = (df == df') && (cols == cols')

instance Eq DataFrame where
    (==) :: DataFrame -> DataFrame -> Bool
    a == b =
        M.keys (columnIndices a) == M.keys (columnIndices b)
            && foldr
                ( \(name, index) acc -> acc && (columns a V.!? index == (columns b V.!? (columnIndices b M.! name)))
                )
                True
                (M.toList $ columnIndices a)

instance Show DataFrame where
    show :: DataFrame -> String
    show d =
        let
            rows = 20
            (r, c) = dataframeDimensions d
            d' =
                d
                    { columns = V.map (takeColumn rows) (columns d)
                    , dataframeDimensions = (min rows r, c)
                    }
            truncationInfo =
                "\n"
                    ++ "Showing "
                    ++ show (min rows r)
                    ++ " rows out of "
                    ++ show r
         in
            T.unpack (asText d' False) ++ (if r > rows then truncationInfo else "")

-- | For showing the dataframe as markdown in notebooks.
toMarkdown :: DataFrame -> T.Text
toMarkdown df = asText df True

-- | For showing the dataframe as a string markdown in notebooks.
toMarkdown' :: DataFrame -> String
toMarkdown' = T.unpack . toMarkdown

asText :: DataFrame -> Bool -> T.Text
asText d properMarkdown =
    let header = map fst (sortBy (compare `on` snd) $ M.toList (columnIndices d))
        types = V.toList $ V.filter (/= "") $ V.map getType (columns d)
        getType :: Column -> T.Text
        showMaybeType :: forall a. (Typeable a) => String
        showMaybeType =
            let s = show (typeRep @a)
             in "Maybe " <> if ' ' `elem` s then "(" <> s <> ")" else s
        getType (BoxedColumn Nothing (_ :: V.Vector a)) = T.pack $ show (typeRep @a)
        getType (BoxedColumn (Just _) (_ :: V.Vector a)) = T.pack $ showMaybeType @a
        getType (UnboxedColumn Nothing (_ :: VU.Vector a)) = T.pack $ show (typeRep @a)
        getType (UnboxedColumn (Just _) (_ :: VU.Vector a)) = T.pack $ showMaybeType @a
        -- Separate out cases dynamically so we don't end up making round trip string
        -- copies.
        get :: Maybe Column -> V.Vector T.Text
        get (Just (BoxedColumn (Just bm) (column :: V.Vector a))) =
            V.generate (V.length column) $ \i ->
                if bitmapTestBit bm i
                    then T.pack (show (Just (V.unsafeIndex column i)))
                    else "Nothing"
        get (Just (BoxedColumn Nothing (column :: V.Vector a))) = case testEquality (typeRep @a) (typeRep @T.Text) of
            Just Refl -> column
            Nothing -> case testEquality (typeRep @a) (typeRep @String) of
                Just Refl -> V.map T.pack column
                Nothing -> V.map (T.pack . show) column
        get (Just (UnboxedColumn (Just bm) column)) =
            let col = V.convert column
             in V.generate (V.length col) $ \i ->
                    if bitmapTestBit bm i
                        then T.pack (show (Just (V.unsafeIndex col i)))
                        else "Nothing"
        get (Just (UnboxedColumn Nothing column)) = V.map (T.pack . show) (V.convert column)
        get Nothing = V.empty
        getTextColumnFromFrame df (i, name) = get $ (V.!?) (columns d) ((M.!) (columnIndices d) name)
        rows =
            transpose $
                zipWith (curry (V.toList . getTextColumnFromFrame d)) [0 ..] header
     in showTable properMarkdown header types rows

-- | O(1) Creates an empty dataframe
empty :: DataFrame
empty =
    DataFrame
        { columns = V.empty
        , columnIndices = M.empty
        , dataframeDimensions = (0, 0)
        , derivingExpressions = M.empty
        }

{- | Safely retrieves a column by name from the dataframe.

Returns 'Nothing' if the column does not exist.

==== __Examples__

>>> getColumn "age" df
Just (UnboxedColumn ...)

>>> getColumn "nonexistent" df
Nothing
-}
getColumn :: T.Text -> DataFrame -> Maybe Column
getColumn name df
    | null df = Nothing
    | otherwise = do
        i <- columnIndices df M.!? name
        columns df V.!? i

{- | Retrieves a column by name from the dataframe, throwing an exception if not found.

This is an unsafe version of 'getColumn' that throws 'ColumnsNotFoundException'
if the column does not exist. Use this when you are certain the column exists.

==== __Throws__

* 'ColumnsNotFoundException' - if the column with the given name does not exist
-}
unsafeGetColumn :: T.Text -> DataFrame -> Column
unsafeGetColumn name df = case getColumn name df of
    Nothing -> throw $ ColumnsNotFoundException [name] "" (M.keys $ columnIndices df)
    Just col -> col

{- | Checks if the dataframe is empty (has no columns).

Returns 'True' if the dataframe has no columns, 'False' otherwise.
Note that a dataframe with columns but no rows is not considered null.
-}
null :: DataFrame -> Bool
null df = V.null (columns df)
