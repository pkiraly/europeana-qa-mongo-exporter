#!/usr/bin/env bash

input=$1

source set-variables.sh

filename="${input##*/}"                      # Strip longest match of */ from start
dir="${input:0:${#input} - ${#filename}}../full" # Substring from 0 thru pos of filename
dir=$(realpath $dir)

if [[ ! -d $dir ]]; then
  mkdir $dir
fi

output=$(printf "%s/%s.json" $dir $filename)
current_time=$(date +%T)
echo "$current_time> $output"

if [[ -e $output ]]; then
  rm $output
fi

while IFS= read -r id; do
  if [[ $id != "" ]]; then
    URL="http://$RECORD_API_URL/europeana-qa/record/$id?dataSource=mongo&withFieldRename=false"
    curl -s "$URL" | jq -c . >> $output
  fi
done < "$1"

current_time=$(date +%T)
