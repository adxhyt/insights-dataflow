#!/bin/bash
hadoop fs -mkdir -p  /user/oozie/share/scripts/
hadoop fs -copyFromLocal -f hql /user/oozie/share
hadoop fs -copyFromLocal -f oozie/workflows /user/oozie/share
hadoop fs -copyFromLocal -f oozie/coordinators /user/oozie/share
hadoop fs -copyFromLocal -f scripts/add_partitions.sh /user/oozie/share/scripts/
hadoop fs -copyFromLocal -f scripts/new_partitions.sh /user/oozie/share/scripts/
