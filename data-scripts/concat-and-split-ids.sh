#!/usr/bin/env bash

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
split -a 4 --numeric-suffixes=1 -l 50000 <(cat ids/part-*) normalized-ids/part-

mv ids _ids
mv normalized-ids ids
