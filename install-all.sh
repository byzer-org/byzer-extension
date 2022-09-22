#!/usr/bin/env bash

ALL_MODUELS="mlsql-shell mlsql-assert mlsql-mllib mlsql-excel byzer-yaml-visualization connect-persist mlsql-ds mlsql-ke mlsql-canal last-command table-repartition"

MODUELS=${1:-""}

if [[ "${MODUELS}" == "" ]];then
   MODUELS=${ALL_MODUELS}
fi

for spark_version in 2.4 3.0 3.3
do
  for module in ${MODUELS}
  do
     ./install.sh ${module} ${spark_version}
  done
done


# ./install.sh ds-hbase-2x
# ./install.sh mlsql-bigdl