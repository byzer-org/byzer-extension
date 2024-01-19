# get script directory
EXTENSION_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# support command line
# ./new_install.sh -name byzer-llm -version 0.1.0 -spark 3.3 
# Initialize variables
MODULE=""
VERSION=""
SPARK="3.3"
RELEASE="false"
SCALA="2.12"

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -module) MODULE="$2"; shift ;;
        -version) VERSION="$2"; shift ;;
        -spark) SPARK="$2"; shift ;;
        -release) RELEASE="true" ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done


if [[ -z "${MODULE}" ]]; then
  echo "please input module name"
  exit 1
fi

# check module is exist or not
if [[ ! -d "${EXTENSION_HOME}/${MODULE}" ]]; then
  echo "module ${MODULE} is not exist"
  exit 1
fi

REMOTE_SERVER=${REMOTE_SERVER:-k8s}

ip=$(ifconfig | grep "inet " | grep -v "127.0.0.1" | awk '{print $2}')

# if the ip address is not  192.168.3.13, then set the
# remote_address as remote （the address is available whe i'am in home）
if [[ $ip != "192.168.3.13" ]];then
    REMOTE_SERVER="remote"
fi

# get the version of sub module
actual_version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout -pl ${MODULE})
if [[ -z "${VERSION}" ]]; then
  VERSION=${actual_version}
fi

if [[ "${VERSION}" != "${actual_version}" ]]; then
  mvn versions:set -DnewVersion=${VERSION} -pl ${MODULE}
fi

# Print the parsed arguments (You can replace this with actual operations)
echo "MODULE: $MODULE"
echo "Version: $VERSION"
echo "Spark Version: $SPARK"
if [[ "$RELEASE" == "true" ]]; then
    echo "Release: Yes"
else
    echo "Release: No"
fi

mvn clean install -Pshade -DskipTests -pl ${MODULE}

# scp ${EXTENSION_HOME}/${MODULE}/target/${MODULE}-${SPARK}_${SCALA}-${VERSION}.jar ${REMOTE_SERVER}:/home/winubuntu/softwares/byzer-lang-all-in-one-linux-amd64-3.3.0-2.3.6/plugin/
# scp ${EXTENSION_HOME}/${MODULE}/target/${MODULE}-${SPARK}_${SCALA}-${VERSION}.jar H:/home/byzerllm/softwares/byzer-lang-all-in-one-linux-amd64-3.3.0-2.3.7/plugin/

if [[ $RELEASE == "true" ]]; then
    object_store_name=$(echo ${MOUDLE} | sed 's/byzer-objectstore-//g')
    prefix="byzer-extensions/nightly-build"
    if [[ "${MOUDLE}" == "byzer-objectstore"* ]]
    then
      prefix="byzer/misc/cloud/${object_store_name}"
    fi

    echo "upload ${EXTENSION_HOME}/${MODULE}/target/${MODULE}-${SPARK}_${SCALA}-${VERSION}.jar to ${prefix}"
    echo "${MODULE}-${SPARK}_${SCALA}-${VERSION}.jar=@${EXTENSION_HOME}/${MODULE}/target/${MODULE}-${SPARK}_${SCALA}-${VERSION}.jar"
    curl --progress-bar \
        -F "${MODULE}-${SPARK}_${SCALA}-${VERSION}.jar=@${EXTENSION_HOME}/${MODULE}/target/${MODULE}-${SPARK}_${SCALA}-${VERSION}.jar" \
      "${BYZER_UPLOADER_URL}&overwrite=true&pathPrefix=${prefix}" | cat
    git tag v$VERSION
    git push gitee v$VERSION
fi
