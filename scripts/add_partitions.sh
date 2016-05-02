#!/bin/bash 

#db_name=$1
#table_name=$2
#namenode_host=$3

HADOOP_CMD=/usr/bin/hadoop

SPARK_CLIENT_CMD=/usr/hdp/current/spark-client/bin/beeline
SPARK_CLIENT_PORT=13001

HIVE_CLIENT_CMD=/usr/hdp/current/hive-client/bin/beeline
HIVE_CLIENT_PORT=10000

CLIENT_CMD=$HIVE_CLIENT_CMD
CLIENT_PORT=$HIVE_CLIENT_PORT


while getopts ":e:sh:t:d:l:" opt; do
  case $opt in
    s)
      CLIENT_CMD=$SPARK_CLIENT_CMD
      CLIENT_PORT=$SPARK_CLIENT_PORT
      ;;
    h)
      namenode_host="$OPTARG"
      ;;
    t)
      table_name="$OPTARG"
      ;;
    d)
      db_name="$OPTARG"
      ;;
    l)
      TABLE_LOCATION="${OPTARG%/}"
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

if [ -z "$namenode_host" ]
then
  echo "Please, provide the name node host name using -h parameter"
fi

CLIENT_CMD_WITH_OPTS="$CLIENT_CMD --silent -u jdbc:hive2://$namenode_host:$CLIENT_PORT" 


if [ -z "${table_name}" ]
then
  echo "Please, provide the table name with -t option"
  exit 1
fi

if [ -z "${namenode_host}" ]
then
  echo "Please, provide the Name Node host name wwith -h option"
  exit 1
fi

if [ ! -z "$db_name" ]
then
  QUALIFIED_TABLE_NAME="${db_name}.${table_name}"
else
  QUALIFIED_TABLE_NAME="${table_name}"
fi

if [ -z "$TABLE_LOCATION" ]
then
  TABLE_LOCATION=`$CLIENT_CMD_WITH_OPTS -e "describe formatted $QUALIFIED_TABLE_NAME" 2>/dev/null | grep Location | sed -rn 's|.*\shdfs://.*:[0-9]+(.*)\s.*|\1|p' | tr -d '[[:space:]]'`
fi

if [ -z "$TABLE_LOCATION" ]
then
  echo "Can't find the data location for $QUALIFIED_TABLE_NAME. Please, provide it with -l option"
  exit 1
fi

echo "Adding partitions for $QUALIFIED_TABLE_NAME from $TABLE_LOCATION"

# Discover partitions - iterate over the file structure and get file
# names - they match partition names
# The first awk part extracts files and excludes directories. Directories have a '-' (dash) where normally the replication factor is
# Sed snippet extracts the part of the path that corresponds to partition structure
# The last AWK piece parses the partition sructure and generates ALTER TABLE statements
PART_FILES=`$HADOOP_CMD fs -ls -R $TABLE_LOCATION/ | grep -v copy | awk '{ if ($2 != "-") print $8 }' | sed -r "s|.*$TABLE_LOCATION/(.*)/[0-9_]+$|\1|g" | tr '/' ','| sort | uniq` 

PART_CNT=`echo "$PART_FILES" | wc -l`

# Replace forward slashes with commmas and include the result in ALTER TABLE statement - yields a working ALTER script
# that works with arbitrary number of partitions
ALTER_SCRIPT=`echo "$PART_FILES" | tr '/' ',' | awk "{ loc=\\$1;gsub(/,/, \"/\", loc);  printf \"ALTER TABLE $QUALIFIED_TABLE_NAME ADD IF NOT EXISTS PARTITION (%s) LOCATION '$TABLE_LOCATION/%s/';\\n\", \\$1, loc }"`

echo $ALTER_SCRIPT > /tmp/alter_script.txt

#$CLIENT_CMD_WITH_OPTS -e "$ALTER_SCRIPT" #2>/dev/null
$CLIENT_CMD_WITH_OPTS -f /tmp/alter_script.txt
rm /tmp/alter_script.txt 

echo "Added $PART_CNT partitions."
