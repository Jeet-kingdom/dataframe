module Main where

import qualified System.Exit as Exit

import Test.HUnit
import Test.QuickCheck (Result, isSuccess, quickCheckWithResult, stdArgs)

import qualified Operations.ReadCsv
import qualified Properties.Csv

tests :: Test
tests = TestList Operations.ReadCsv.tests

allSuccessful :: [Result] -> Bool
allSuccessful = all isSuccess

main :: IO ()
main = do
    result <- runTestTT tests
    if failures result > 0 || errors result > 0
        then Exit.exitFailure
        else do
            propResults <-
                mapM (quickCheckWithResult stdArgs) Properties.Csv.tests
            if allSuccessful propResults
                then Exit.exitSuccess
                else Exit.exitFailure
