module DataFrame.IO.Parquet.Writer (writeParquet) where

import qualified Data.ByteString.Builder as B
import System.IO (IOMode(WriteMode), withBinaryFile)

-- Using the path we found with grep
import DataFrame.Internal.DataFrame (DataFrame)

-- | Minimalist implementation to verify the build system works
writeParquet :: FilePath -> DataFrame -> IO ()
writeParquet path _df = do
    putStrLn $ "GSoC Issue #181: Opening " ++ path
    withBinaryFile path WriteMode $ \handle -> do
        -- Every Parquet file MUST start and end with these 4 bytes
        B.hPutBuilder handle (B.string8 "PAR1")
        
        -- (Logic for RowGroups will go here later)
        
        B.hPutBuilder handle (B.string8 "PAR1")
    putStrLn "Successfully wrote Parquet magic bytes."