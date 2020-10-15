#!/usr/bin/env bash

# counts total records, and total bytes
#
# usage: $0 v2020-05

VERSION=$1

if [[ "$VERSION" = "" || ! -d $VERSION || ! -e ${VERSION}/metadata.csv ]]; then
  echo "Invalid version: '${VERSION}'"
  exit 1;
fi

COUNT=$(grep -c total ${VERSION}/metadata.csv)
if [ $COUNT -eq 1 ]; then
  echo "This file has been already altered"
  exit 1
fi

cat ${VERSION}/metadata.csv | cut -d"," -f1 | paste -sd+ | bc > total-lines
cat ${VERSION}/metadata.csv | cut -d"," -f2 | paste -sd+ | bc > total-bytes
cat total-lines total-bytes <(echo "total") | paste -sd, >> ${VERSION}/metadata.csv

echo "The RESULT:"
tail -n 3 ${VERSION}/metadata.csv
