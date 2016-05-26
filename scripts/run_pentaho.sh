#!/bin/bash 
#
# Ingest zipped XML files received from HomeNet/AIS, convert to csv, and create relevant tables
#

UNZIP_CMD=/usr/bin/unzip
PENTAHO_CMD=/opt/data-integration/kitchen.sh
HADOOP_CMD=/usr/bin/hadoop
FLAT_XML_PATH="/data/database/ais"

while getopts "a:l:" opt; do
  case $opt in
    a)
      AIS_SOURCE="$OPTARG"
      ;;
    l)
      SRC_LOCATION="$OPTARG"
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

if [ -z "$SRC_LOCATION" ]
then
  echo "Please, provide the source data location using -l parameter"
  exit 1
fi

if [ -z "$AIS_SOURCE" ]
then
  AIS_SOURCE=pentaho/ais.kjb
fi

function xml_to_csv(){
  file_source=$1

  echo "Converting files in dir:" $file_source " from:" $AIS_SOURCE
  $PENTAHO_CMD -file=$AIS_SOURCE -level=Minimal -param:xml_input_dir=$file_source
}

function hadoop_put(){
  file_name=$1
  echo "filename: " $file_name
  directory_name=${file_name##*/}
  directory_name=${directory_name%.*}
  directory_name=${directory_name##*.}
  echo "directory_name: " $directory_name

  echo "putting $file_name into $FLAT_XML_PATH/$directory_name/"
  $HADOOP_CMD fs -put "$file_name" "$FLAT_XML_PATH/$directory_name/"
}

for i in "$SRC_LOCATION"*.zip
do
  $UNZIP_CMD -d "$SRC_LOCATION" "$i"
  xml_to_csv "$SRC_LOCATION"
  for j in "$SRC_LOCATION"*.xml
  do
    for k in "$SRC_LOCATION"*.csv
    do
      hadoop_put "$k"
      rm "$k"
    done
    rm "$j"
  done
done