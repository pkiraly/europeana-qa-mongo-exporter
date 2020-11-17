#!/usr/bin/env bash

# Redistribute IDs evenly
#
# The Spark process distributes the ids into files unevenly, which has a heavy
# effect on the parallel download. This script concatenates the IDs, and split
# them into files each contain 50K items
#
# usage: $0 VERSION

VERSION=$1
if [[ "$VERSION" == "" ]]; then
  echo "Please give a version"
  exit 1
fi

if [[ ! -d $VERSION ]]; then
  echo "$VERSION is not existing"
  exit 1
fi

cd $VERSION

if [[ -d "normalized-ids" ]]; then
  echo "normalized-ids is already existing"
  exit 1
fi

mkdir normalized-ids
# concatenate and split the id files.
# -l = Each new file contains 50K ids,
# -a = the count number's length is 4 characters
# --numeric-suffixes=1 = the start value is 1
split -a 4 --numeric-suffixes=1 -l 50000 <(cat ids/part-*) normalized-ids/part-

mv ids _ids
mv normalized-ids ids
