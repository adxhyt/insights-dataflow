#!/bin/bash 
#
# Ingest zipped XML files, received from HomeNet/AIS and create relevant tables
#

HADOOP_CMD=/usr/bin/hadoop
UNZIP_CMD=/usr/bin/unzip

SPARK_CLIENT_CMD=/usr/hdp/current/spark-client/bin/beeline
SPARK_CLIENT_PORT=13001

HIVE_CLIENT_CMD=/usr/hdp/current/hive-client/bin/beeline
HIVE_CLIENT_PORT=10000

CLIENT_CMD=$HIVE_CLIENT_CMD
CLIENT_PORT=$HIVE_CLIENT_PORT

FLAT_XML_PATH="/data/database/ais"
FLAT_XML_FILE="$FLAT_XML_PATH/ais.xml"

PENTAHO_CMD=/usr/data-integration/kitchen.sh

while getopts "sh:l:" opt; do
  case $opt in
    s)
      CLIENT_CMD=$SPARK_CLIENT_CMD
      CLIENT_PORT=$SPARK_CLIENT_PORT
      ;;
    h)
      NAMENODE_HOST="$OPTARG"
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

#if [ -z "$NAMENODE_HOST" ]
#then
#  echo "Please, provide the name node host name using -h parameter"
#  exit 1
#fi

if [ -z "$SRC_LOCATION" ]
then
  echo "Please, provide the source data location using -l parameter"
fi

CLIENT_CMD_WITH_OPTS="$CLIENT_CMD --silent -u jdbc:hive2://$namenode_host:$CLIENT_PORT" 


function extract_concat_zip(){
  extc_src=$1
  extc_dst=$2

  $UNZIP_CMD -p -a "$extc_src" | $HADOOP_CMD fs -appendToFile - "$extc_dst"
}

function xml_to_csv(){
  file_source=$1
  extc_dst=$2

  $PENTAHO_CMD -level=Minimal -xml_input_dir=$file_source
}

$HADOOP_CMD fs -rm -skipTrash "$FLAT_XML_FILE"

#  cfile="/mnt/resource/ais/RMS_AISDataSet_10012015_131824.zip"
for i in "$SRC_LOCATION"*.zip
do
  extract_concat_zip "$i" "$FLAT_XML_FILE"
done

for i in "$SRC_LOCATION"*.zip
do
  $UNZIP_CMD "$i"
  for j in "$SRC_LOCATION"*.xml
  do
    xml_to_csv "$j"
    for k in "$SRC_LOCATION"*.csv
    do
      $HADOOP_CMD dfs -put "$k" "$FLAT_XML_PATH"
      rm "$k"
    done
    rm "$j"
  done
done


