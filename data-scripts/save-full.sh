#!/usr/bin/env bash

VERSION=$1
SECONDS=0

source set-variables.sh

WORKING_DIR=$(pwd)

BASE_TARGET_DIR=$DATA_ARCHIVE_DIR/${VERSION}
TARGET_DIR=${BASE_TARGET_DIR}/full/

if [[ ! -d $TARGET_DIR ]]; then
  mkdir ${TARGET_DIR}
fi

cp $VERSION/metadata.* ${BASE_TARGET_DIR}

for FILE in $VERSION/full/*.gz; do
  echo $FILE
  cp ${FILE} ${TARGET_DIR}
  sleep 1
done

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%s %s> prepare-download for version %s DONE\n" $(date +"%F %T") $VERSION
printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
