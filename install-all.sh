#!/usr/bin/env bash

ALL_MODUELS="mlsql-shell mlsql-assert mlsql-mllib mlsql-excel byzer-yaml-visualization connect-persist mlsql-ds last-command table-repartition"

MODUELS=${1:-""}

if [[ "${MODUELS}" == "" ]];then
   MODUELS=${ALL_MODUELS}
fi

for spark_version in spark330
do
  for module in ${MODUELS}
  do
     ./install.sh ${module} 3.3
  done
done


# ./install.sh ds-hbase-2x
# ./install.sh mlsql-bigdl