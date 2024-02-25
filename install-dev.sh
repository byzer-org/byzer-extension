user_home=$(echo ~)
PROJECT=${user_home}/projects/byzer-extension
REMOTE_SERVER=${REMOTE_SERVER:-k8s}

ip=$(ifconfig | grep "inet " | grep -v "127.0.0.1" | awk '{print $2}')

# if the ip address is not  192.168.3.13, then set the
# remote_address as remote （the address is available whe i'am in home）
if [[ $ip != "192.168.3.13" ]];then
    REMOTE_SERVER="remote"
fi

MOUDLE_NAME=$1


# use mvn command to get sub module byzer-llm version
OLD_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout -pl byzer-llm)
# check the VERSION is null or not
if [[ -z "${VERSION}" ]]; then
  VERSION=${OLD_VERSION}
fi

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
#mlsql_plugin_tool upload \
#--module_name ${MOUDLE_NAME}  \
#--user ${STORE_USER}        \
#--password ${STORE_PASSWORD} \
#--jar_path ${PROJECT}/${MOUDLE_NAME}/build/${MOUDLE_NAME}-${MIDDLE}-${VERSION}.jar


## if MOUDLE_NAME starts with byzer-objectstore-xxx, we will upload it to byzer/misc/cloud/xxx
## otherwise we will upload it to byzer-extensions/nightly-build
object_store_name=$(echo ${MOUDLE_NAME} | sed 's/byzer-objectstore-//g')
prefix="byzer-extensions/nightly-build"
if [[ "${MOUDLE_NAME}" == "byzer-objectstore"* ]]
then
  prefix="byzer/misc/cloud/${object_store_name}"
fi

echo "scp ${PROJECT}/${MOUDLE_NAME}/build/${MOUDLE_NAME}-${MIDDLE}-${VERSION}.jar to remote server"
scp ${PROJECT}/${MOUDLE_NAME}/build/${MOUDLE_NAME}-${MIDDLE}-${VERSION}.jar ${REMOTE_SERVER}:/home/winubuntu/softwares/byzer-lang-all-in-one-linux-amd64-3.3.0-2.3.9/plugin/
scp ${PROJECT}/${MOUDLE_NAME}/build/${MOUDLE_NAME}-${MIDDLE}-${VERSION}.jar H:/home/byzerllm/softwares/byzer-lang-all-in-one-linux-amd64-3.3.0-2.3.7/plugin/
#scp ${PROJECT}/${MOUDLE_NAME}/build/${MOUDLE_NAME}-${MIDDLE}-${VERSION}.jar H3:/home/byzerllm/softwares/byzer-lang-all-in-one-linux-amd64-3.3.0-2.3.8/plugin/

#echo "clean old version byzer-llm extension"
#ssh -t  ${REMOTE_SERVER} "rm  /home/winubuntu/softwares/byzer-lang-all-in-one-linux-amd64-3.3.0-2.3.6/plugin/${MOUDLE_NAME}-${MIDDLE}-${OLD_VERSION}.jar"
#ssh -t  H "rm  /home/winubuntu/softwares/byzer-lang-all-in-one-linux-amd64-3.3.0-2.3.7/plugin/${MOUDLE_NAME}-${MIDDLE}-${OLD_VERSION}.jar"
#ssh -t  H3 "rm  /home/winubuntu/softwares/byzer-lang-all-in-one-linux-amd64-3.3.0-2.3.8/plugin/${MOUDLE_NAME}-${MIDDLE}-${OLD_VERSION}.jar"
#curl --progress-bar \
#    -F "${MOUDLE_NAME}-${MIDDLE}-${VERSION}.jar=@${PROJECT}/${MOUDLE_NAME}/build/${MOUDLE_NAME}-${MIDDLE}-${VERSION}.jar" \
#  "${BYZER_UPLOADER_URL}&overwrite=true&pathPrefix=${prefix}" | cat



