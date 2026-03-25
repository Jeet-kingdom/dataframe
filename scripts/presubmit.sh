#!/bin/bash

set -e

./scripts/format.sh
./scripts/lint.sh

cabal build
cabal test

cd ../dataframe-persistent

cabal build

cd ../dataframe-hasktorch

cabal build

cd examples

cabal build all
