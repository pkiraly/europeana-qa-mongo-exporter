#!/usr/bin/env bash

task(){
  ./download-ids.sh $1
}

get_running_tasks_count(){
  task_count=$(ps aux | grep -c "[d]ownload-ids.sh")
}

dir=$1
files=$(ls $dir/part*)

open_sem(){
  mkfifo pipe-$$
  exec 3<>pipe-$$
  rm pipe-$$
  local i=$1
  for((;i>0;i--)); do
    printf %s 000 >&3
  done
}

run_with_lock(){
  local x
  read -u 3 -n 3 x && ((0==x)) || exit $x
  (
    ( "$@"; )
    printf '%.3d' $? >&3
  )&
}

N=6
open_sem $N
for file in $files; do
  output=$(echo "${file}" | sed 's/ids/full/' | sed 's/$/.json/')
  if [[ -e $output ]]; then
    size=$(stat --printf="%s" $output)
  else
    size="0"
  fi

  if [[ "$size" = "0" ]]; then
    run_with_lock task $file
  fi
done

get_running_tasks_count
while [[ $task_count != 0 ]]; do
  echo "wait a bit"
  sleep 10
  get_running_tasks_count
done

duration=$SECONDS
hours=$(($duration / (60*60)))
mins=$(($duration % (60*60) / 60))
secs=$(($duration % 60))

date +"%F %T"
echo "$time> download all DONE"
printf "%02d:%02d:%02d elapsed.\n" $hours $mins $secs
