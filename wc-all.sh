#!/usr/bin/env bash

#
# compare the number of lines in the ID lists and of the record files.
#

SECONDS=0
# VERSION=v2019-08
VERSION=$1

source set-variables.sh

for i in {0..11}
do
  j=$(printf "%02d" $i)
  time=$(date +%T)
  echo "$time> $j"
  echo "count lines of ID"
  wc -l $DATA_EXPORT_DIR/$VERSION/ids/part-0${j}* > wc-csv-${j}.txt
  echo "count lines of records"
  wc -l $DATA_EXPORT_DIR/$VERSION/full/part-0${j}* > wc-json-${j}.txt
  echo "now wait a bit"
  sleep 5s
done

# aggregate and clean ID lists
cat wc-csv-*.txt | sed "s,$DATA_EXPORT_DIR/$VERSION/ids/,," | grep -v total | awk '{print $2,$1}' > wc-csv.txt

# aggregate and clean record list
cat wc-json-*.txt | sed "s,$DATA_EXPORT_DIR/$VERSION/full/,," | sed 's,.json,,' | grep -v total | awk '{print $2,$1}' > wc-json.txt

# compare
diff wc-csv.txt wc-json.txt > wc-diff.txt

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

date +"%T"
echo "$time> check IDs DONE"
printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
