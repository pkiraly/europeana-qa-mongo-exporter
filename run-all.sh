#!/usr/bin/env bash

VERSION=$1
if [[ "$VERSION" = "" ]]; then
  echo "Specify a version."
  exit 1
fi

SECONDS=0
source set-variables.sh
CURRENT_DIR=$(pwd)

# echo "WARNING: the first step is commented out. Please uncomment it next time."
# download ids
./run-mongo-to-json.sh $VERSION

# normalize id files
cd $DATA_EXPORT_DIR
./concat-and-split-ids.sh $VERSION
cd $CURRENT_DIR

# download full files
./download-all.sh $DATA_EXPORT_DIR/$VERSION/ids

# count lines
./wc-all.sh $VERSION

grep ">" wc-diff.txt | awk '{print $2}' | xargs -I "@@" ./make-diff.sh @@ $VERSION  | grep "/" > id-diff.txt

./cp-ids.sh $VERSION
# ./cp-full.sh $VERSION

cd $DATA_EXPORT_DIR
echo "Compressing. Check $DATA_EXPORT_DIR/count-and-compress-$VERSION.log"
./count-and-compress.sh $VERSION > count-and-compress-${VERSION}.log
echo "Copying. Check $DATA_EXPORT_DIR/save-full-${VERSION}.log"
./save-full.sh $VERSION > save-full-${VERSION}.log
cd $CURRENT_DIR

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

date +"%F %T"
echo "$time> run all DONE"
printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
