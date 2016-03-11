#!/bin/bash

sudo easy_install  quik 
sudo su -m -l oozie -c '/usr/hdp/current/oozie-server/bin/oozie-setup.sh sharelib create -fs /user/oozie/share/lib/' 
libpath=`sudo su -m -l oozie -c "oozie admin -sharelibupdate | grep sharelibDirNew | sed 's/.*oozie\/share\/lib\/\(.*\)/\1/'"` #find out where is the sharelib
sudo mkdir -p /home/oozie/insights-dataflow/oozie/
sudo cp -r oozie/configs /home/oozie/insights-dataflow/oozie/
sudo chown -R oozie /home/oozie/insights-dataflow/oozie
hadoop fs -copyFromLocal -f oozie/sharelib/*  /user/oozie/share/lib/$libpath/ 
hadoop fs -copyFromLocal -f scripts/*  /user/oozie/share/scripts/
hadoop fs -copyFromLocal -f /usr/hdp/current/hive-client/conf/hive-site.xml /tmp/hive-site.xml
hadoop fs -copyFromLocal -f /usr/hdp/current/hive-client/conf/hive-site.xml /tmp/oozie-hive-site.xml
hadoop fs -copyFromLocal -f oozie/standalone_jars/* /user/oozie/share/lib
sudo su -m -l oozie -c 'oozie admin -sharelibupdate' #update the sharlib for real


