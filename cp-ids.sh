#!/usr/bin/env bash

VERSION=$1

source set-variables.sh

SOURCE_DIR=$DATA_EXPORT_DIR/$VERSION/ids
TARGET_DIR=$DATA_ARCHIVE_DIR/$VERSION/ids
SECONDS=0

if [[ "$VERSION" = "" ]]; then
  echo "Please specify a version!"
  exit 1;
fi

if [[ ! -e $TARGET_DIR ]]; then
  echo "Creating $TARGET_DIR"
  mkdir -p $TARGET_DIR
  # exit 1;
fi

FILES=$(ls $SOURCE_DIR/part*)

for FILE in $FILES; do
  echo $FILE
  cp $FILE $TARGET_DIR
  sleep 1
done

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

date +"%F %T"
echo "$time> run all DONE"
printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
