#!/usr/bin/env bash

VERSION=$1
SECONDS=0

printf "%s %s> prepare-download for version %s START\n" $(date +"%F %T") $VERSION

source set-variables.sh

WORKING_DIR=$(pwd)
SOURCE_DIR=$DATA_EXPORT_DIR/$VERSION
TARGET_DIR=$DATA_ARCHIVE_DIR/$VERSION
FULL=$SOURCE_DIR/full
METADATA_TXT=$SOURCE_DIR/metadata.txt
METADATA_CSV=$SOURCE_DIR/metadata.csv

if [[ -e ${METADATA_TXT} ]]; then
  rm -r ${METADATA_TXT}
fi

echo ${METADATA_TXT}

printf "%s %s> Counting\n" $(date +"%F %T")
for i in {000..150}; do
  printf "%s %s> count/%s\n" $(date +"%F %T") $i
  count=$(ls -la ${FULL}/part-${i}?.json 2> /dev/null | wc -l)
  echo $count
  if [[ $count -gt 0 ]]; then
    echo "wc -l -c ${FULL}/part-${i}?.json >> ${METADATA_TXT}"
    wc -l -c ${FULL}/part-${i}?.json >> ${METADATA_TXT}
  fi
done

printf "%s %s> Create metadata.csv\n" $(date +"%F %T")
grep -v total ${METADATA_TXT} | awk '{print $1","$2","$3}' | sed 's/,\/.*\//,/' > ${METADATA_CSV}
. ./count-totals.sh $VERSION

printf "%s %s> compressing\n" $(date +"%F %T")
cd $FULL
for i in {000..150}; do
  printf "%s %s> compress %s\n" $(date +"%F %T") $i
  count=$(ls -la part-${i}?.json 2> /dev/null | wc -l)
  if [[ $count -gt 0 ]]; then
    gzip part-${i}?.json;
  fi
done

cd $WORKING_DIR

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

printf "%s %s> prepare-download for version %s DONE\n" $(date +"%F %T") $VERSION
printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
