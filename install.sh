conda activate dev
PROJECT=/Users/allwefantasy/projects/byzer-extension

MOUDLE_NAME=$1
VERSION=${VERSION:-"0.1.0-SNAPSHOT"}
V=${2:-3.3}
MIDDLE="2.4_2.11"

SPARK="spark330"

if [[ "${V}" == "2.4" ]]
then
   SPARK=spark243
elif [ "${V}" == "3.0" ]; then
   SPARK=spark311
elif [ "${V}" == "3.3" ]; then
   SPARK=spark330
fi


if [[ "${SPARK}" == "spark330" || "${SPARK}" == "spark311" ]]
then
   MIDDLE="${V}_2.12"
fi

echo ${MOUDLE_NAME}
echo ${SPARK}
echo ${MIDDLE}

mlsql_plugin_tool build --module_name ${MOUDLE_NAME} --spark ${SPARK}
mlsql_plugin_tool upload \
--module_name ${MOUDLE_NAME}  \
--user ${STORE_USER}        \
--password ${STORE_PASSWORD} \
--jar_path ${PROJECT}/${MOUDLE_NAME}/build/${MOUDLE_NAME}-${MIDDLE}-${VERSION}.jar


## if MOUDLE_NAME starts with byzer-objectstore-xxx, we will upload it to byzer/misc/cloud/xxx
## otherwise we will upload it to byzer-extensions/nightly-build
object_store_name=$(echo ${MOUDLE_NAME} | sed 's/byzer-objectstore-//g')
prefix="byzer-extensions/nightly-build"
if [[ "${MOUDLE_NAME}" == "byzer-objectstore"* ]]
then
  prefix="byzer/misc/cloud/${object_store_name}"
fi

echo "upload ${PROJECT}/${MOUDLE_NAME}/build/${MOUDLE_NAME}-${MIDDLE}-${VERSION}.jar to ${prefix}"
curl --progress-bar \
    -F "${MOUDLE_NAME}-${MIDDLE}-${VERSION}.jar=@${PROJECT}/${MOUDLE_NAME}/build/${MOUDLE_NAME}-${MIDDLE}-${VERSION}.jar" \
  "${BYZER_UPLOADER_URL}&overwrite=true&pathPrefix=${prefix}" | cat



