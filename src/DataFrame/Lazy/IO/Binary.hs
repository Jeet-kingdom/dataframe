{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Simple column-oriented binary spill format (DFBN).

Layout (all integers little-endian):

@
[magic:       4  bytes] "DFBN"
[num_columns: 4  bytes] Word32
  per column:
    [name_len:  2  bytes] Word16  (byte length of UTF-8 name)
    [name:     name_len bytes]
    [type_tag:  1  byte]  Word8
[num_rows:    8  bytes] Word64

per column data block (order matches schema):
  type_tag 0 (Int):            num_rows × Int64 LE
  type_tag 1 (Double):         num_rows × Double LE (IEEE 754)
  type_tag 2 (Text):           (num_rows+1) × Word32 offsets  ++  payload bytes (UTF-8)
  type_tag 3 (Maybe Int):      ceil(num_rows/8)-byte null bitmap  ++  num_rows × Int64 LE
  type_tag 4 (Maybe Double):   ceil(num_rows/8)-byte null bitmap  ++  num_rows × Double LE
  type_tag 5 (Maybe Text):     ceil(num_rows/8)-byte null bitmap
                                ++  (num_rows+1) × Word32 offsets  ++  payload bytes
@

Null bitmap: bit @i@ of byte @i\/8@ is 1 when row @i@ is non-null.
-}
module DataFrame.Lazy.IO.Binary (
    spillToDisk,
    readSpilled,
    withSpilled,
) where

import Control.Exception (SomeException, bracket, try)
import Control.Monad
import qualified Data.Binary.Get as G
import qualified Data.Binary.Put as P
import qualified Data.ByteString.Lazy as BL
import qualified Data.List as L
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Data.Bits (setBit, testBit)
import Data.Maybe (fromMaybe, isJust)
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import Data.Word (Word16, Word32, Word64, Word8)
import DataFrame.Internal.Column (Column (..))
import DataFrame.Internal.DataFrame (DataFrame (..))
import System.Directory (getTemporaryDirectory, removeFile)
import System.IO (hClose, openTempFile)
import Type.Reflection (typeRep)

-- ---------------------------------------------------------------------------
-- Type tags
-- ---------------------------------------------------------------------------

tagInt, tagDouble, tagText, tagMaybeInt, tagMaybeDouble, tagMaybeText :: Word8
tagInt = 0
tagDouble = 1
tagText = 2
tagMaybeInt = 3
tagMaybeDouble = 4
tagMaybeText = 5

-- ---------------------------------------------------------------------------
-- Write
-- ---------------------------------------------------------------------------

-- | Serialise a 'DataFrame' to a DFBN binary file.
spillToDisk :: FilePath -> DataFrame -> IO ()
spillToDisk path df = BL.writeFile path (P.runPut (putDataFrame df))

putDataFrame :: DataFrame -> P.Put
putDataFrame df = do
    -- Magic
    P.putByteString "DFBN"
    -- Schema
    let names =
            fmap
                fst
                (L.sortBy (\a b -> compare (snd a) (snd b)) (M.toList (columnIndices df)))
    let ncols = fromIntegral (length names) :: Word32
    P.putWord32le ncols
    let cols = V.toList (columns df)
    mapM_ (uncurry putColumnSchema) (zip names cols)
    -- Row count
    let nrows = fromIntegral (fst (dataframeDimensions df)) :: Word64
    P.putWord64le nrows
    -- Column data
    mapM_ (putColumnData (fst (dataframeDimensions df))) cols

putColumnSchema :: T.Text -> Column -> P.Put
putColumnSchema name col = do
    let nameBytes = TE.encodeUtf8 name
    P.putWord16le (fromIntegral (BL.length (BL.fromStrict nameBytes)) :: Word16)
    P.putByteString nameBytes
    P.putWord8 (columnTypeTag col)

columnTypeTag :: Column -> Word8
columnTypeTag (UnboxedColumn (_ :: VU.Vector a)) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl -> tagInt
        Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
            Just Refl -> tagDouble
            Nothing -> error "spillToDisk: unsupported UnboxedColumn element type"
columnTypeTag (BoxedColumn _) = tagText
columnTypeTag (OptionalColumn (_ :: V.Vector (Maybe a))) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl -> tagMaybeInt
        Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
            Just Refl -> tagMaybeDouble
            Nothing -> tagMaybeText

putColumnData :: Int -> Column -> P.Put
putColumnData _ (UnboxedColumn (v :: VU.Vector a)) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl ->
            VU.mapM_ (\x -> P.putInt64le (fromIntegral (x :: Int))) v
        Nothing ->
            case testEquality (typeRep @a) (typeRep @Double) of
                Just Refl ->
                    VU.mapM_ (\x -> P.putDoublele (x :: Double)) v
                Nothing ->
                    error "spillToDisk: unsupported UnboxedColumn element type"
putColumnData _ (BoxedColumn (v :: V.Vector a)) =
    case testEquality (typeRep @a) (typeRep @T.Text) of
        Just Refl -> putTextVector v
        Nothing -> error "spillToDisk: unsupported BoxedColumn element type"
putColumnData _ (OptionalColumn (v :: V.Vector (Maybe a))) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl -> do
            putNullBitmap (V.map isJust v)
            V.mapM_ (\mx -> P.putInt64le (fromIntegral (fromMaybe 0 mx :: Int))) v
        Nothing ->
            case testEquality (typeRep @a) (typeRep @Double) of
                Just Refl -> do
                    putNullBitmap (V.map isJust v)
                    V.mapM_ (\mx -> P.putDoublele (fromMaybe 0.0 mx :: Double)) v
                Nothing -> do
                    -- Maybe Text or other Show-able type
                    let showText x = case testEquality (typeRep @a) (typeRep @T.Text) of
                            Just Refl -> x
                            Nothing -> T.pack (show x)
                    let texts = V.map (maybe T.empty showText) v
                    putNullBitmap (V.map isJust v)
                    putTextVector texts

-- | Write a null-validity bitmap: 1 bit per row, packed LSB-first into bytes.
putNullBitmap :: V.Vector Bool -> P.Put
putNullBitmap valids = mapM_ P.putWord8 bytes
  where
    n = V.length valids
    numBytes = (n + 7) `div` 8
    bytes =
        [ foldr
            ( \bit acc ->
                let row = byteIdx * 8 + bit
                 in if row < n && (valids V.! row)
                        then setBit acc bit
                        else acc
            )
            (0 :: Word8)
            [0 .. 7]
        | byteIdx <- [0 .. numBytes - 1]
        ]

-- | Write a Text vector: (num_rows+1) Word32 offsets followed by UTF-8 payload.
putTextVector :: V.Vector T.Text -> P.Put
putTextVector v = do
    let encoded = V.map TE.encodeUtf8 v
    let offsets =
            V.scanl
                (\acc bs -> acc + fromIntegral (BL.length (BL.fromStrict bs)))
                (0 :: Word32)
                encoded
    V.mapM_ P.putWord32le offsets
    V.mapM_ P.putByteString encoded

-- ---------------------------------------------------------------------------
-- Read
-- ---------------------------------------------------------------------------

-- | Deserialise a DFBN binary file into a 'DataFrame'.
readSpilled :: FilePath -> IO DataFrame
readSpilled path = do
    bs <- BL.readFile path
    case G.runGetOrFail getDataFrame bs of
        Left (_, _, err) -> fail ("readSpilled: " <> err)
        Right (_, _, df) -> return df

getDataFrame :: G.Get DataFrame
getDataFrame = do
    magic <- G.getByteString 4
    when (magic /= "DFBN") $ fail "readSpilled: bad magic bytes"
    ncols <- fromIntegral <$> G.getWord32le
    schema <- mapM (const getColumnSchema) [1 .. ncols :: Int]
    nrows <- fromIntegral <$> G.getWord64le
    cols <- mapM (getColumnData nrows . snd) schema
    let names = fmap fst schema
    return $
        DataFrame
            { columns = V.fromList cols
            , columnIndices = M.fromList (zip names [0 ..])
            , dataframeDimensions = (nrows, ncols)
            , derivingExpressions = M.empty
            }

getColumnSchema :: G.Get (T.Text, Word8)
getColumnSchema = do
    nameLen <- fromIntegral <$> G.getWord16le
    nameBytes <- G.getByteString nameLen
    tag <- G.getWord8
    return (TE.decodeUtf8 nameBytes, tag)

getColumnData :: Int -> Word8 -> G.Get Column
getColumnData nrows tag
    | tag == tagInt = do
        vals <- mapM (const (fromIntegral <$> G.getInt64le)) [1 .. nrows]
        return $ UnboxedColumn (VU.fromList (vals :: [Int]))
    | tag == tagDouble = do
        vals <- mapM (const G.getDoublele) [1 .. nrows]
        return $ UnboxedColumn (VU.fromList (vals :: [Double]))
    | tag == tagText = do
        offsets <- mapM (const (fromIntegral <$> G.getWord32le)) [0 .. nrows]
        let sizes = zipWith (-) (tail offsets) offsets
        texts <- mapM (fmap TE.decodeUtf8 . G.getByteString) sizes
        return $ BoxedColumn (V.fromList texts)
    | tag == tagMaybeInt = do
        bitmap <- getNullBitmap nrows
        vals <- mapM (const (fromIntegral <$> G.getInt64le)) [1 .. nrows]
        let maybes = zipWith (\valid v -> if valid then Just (v :: Int) else Nothing) bitmap vals
        return $ OptionalColumn (V.fromList maybes)
    | tag == tagMaybeDouble = do
        bitmap <- getNullBitmap nrows
        vals <- mapM (const G.getDoublele) [1 .. nrows]
        let maybes =
                zipWith (\valid v -> if valid then Just (v :: Double) else Nothing) bitmap vals
        return $ OptionalColumn (V.fromList maybes)
    | tag == tagMaybeText = do
        bitmap <- getNullBitmap nrows
        offsets <- mapM (const (fromIntegral <$> G.getWord32le)) [0 .. nrows]
        let sizes = zipWith (-) (tail offsets) offsets
        texts <- mapM (fmap TE.decodeUtf8 . G.getByteString) sizes
        let maybes = zipWith (\valid t -> if valid then Just t else Nothing) bitmap texts
        return $ OptionalColumn (V.fromList maybes)
    | otherwise = fail ("readSpilled: unknown type tag " <> show tag)

-- | Read a null-bitmap for @nrows@ rows (ceil(nrows/8) bytes).
getNullBitmap :: Int -> G.Get [Bool]
getNullBitmap nrows = do
    let numBytes = (nrows + 7) `div` 8
    bytes <- mapM (const G.getWord8) [1 .. numBytes]
    return $
        take
            nrows
            [ testBit (bytes !! (row `div` 8)) (row `mod` 8)
            | row <- [0 ..]
            ]

-- ---------------------------------------------------------------------------
-- bracket helper
-- ---------------------------------------------------------------------------

{- | Spill a DataFrame to a temporary file, run an action with the path,
then delete the file even if the action throws.
-}
withSpilled :: DataFrame -> (FilePath -> IO a) -> IO a
withSpilled df action = do
    tmpDir <- getTemporaryDirectory
    bracket
        ( do
            (path, h) <- openTempFile tmpDir "dataframe_spill.dfbn"
            hClose h
            spillToDisk path df
            return path
        )
        (\path -> void (try (removeFile path) :: IO (Either SomeException ())))
        action
