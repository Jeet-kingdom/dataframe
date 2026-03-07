{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import qualified Data.Map as M
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Lazy as L

import Data.Text (Text)
import Data.Time
import DataFrame.Internal.Schema (Schema (..), schemaType)
import DataFrame.Operators

main :: IO ()
main = do
    let city = F.col @Text "City"
    let measurement = F.col @Double "Measurement"
    let schema =
            Schema $
                M.fromList
                    [ (F.name city, schemaType @Text)
                    , (F.name measurement, schemaType @Double)
                    ]

    startCalculation <- getCurrentTime
    df <-
        L.scanSeparated ';' schema "../data/measurements.txt"
            |> L.groupBy
                [F.name city]
                [ F.minimum measurement `as` "minimum"
                , F.mean measurement `as` "mean"
                , F.maximum measurement `as` "maximum"
                ]
            |> L.runDataFrame
    print $ D.sortBy [D.Asc city] df
    endCalculation <- getCurrentTime
    let calculationTime = diffUTCTime endCalculation startCalculation
    putStrLn $ "Calculation Time: " ++ show calculationTime
