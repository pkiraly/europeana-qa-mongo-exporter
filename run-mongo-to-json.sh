#!/usr/bin/env bash

if [ "$1" = "build" ]; then
  git pull
  mvn clean install
fi

THREADS=1
VERSION=$1
if [[ "$VERSION" = "" ]]; then
  echo "Specify a version."
  exit 1
fi

source set-variables.sh

BASEDIR=$DATA_EXPORT_DIR/$VERSION
if [ ! -d $BASEDIR ]; then
  mkdir -p $BASEDIR
fi

OUTPUT=${BASEDIR}/ids
if [ -d ${OUTPUT} ]; then
  rm -rf ${OUTPUT}
fi

echo "output: ${OUTPUT}"

spark-submit \
  --driver-memory 3g --executor-memory 3g \
  --class de.gwdg.metadataqa.mongo.MongoToJson \
  --master local[$THREADS] \
  target/europeana-qa-mongo-exporter-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --recordAPIUrl ${RECORD_API_URL} \
  --outputFileName ${OUTPUT} \
  --mongoHost ${MONGO_HOST} \
  --mongoPort ${MONGO_PORT} \
  --mongoDatabase ${MONGO_DATABASE} \
  --idsOnly

rm ${OUTPUT}/\.*.crc
rm ${OUTPU}/_SUCCESS

