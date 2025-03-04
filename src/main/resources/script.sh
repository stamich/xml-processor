#!/bin/bash

set -x
set -e

ABS_BASE_PATH="$(realpath ${BASH_SOURCE[0]})"
CAAR_APP_HOME="${ABS_BASE_PATH%/*/*}"
SSH_CONFIG_FILE=$(realpath ${SSH_CONFIG_FILE:-"${CAAR_APP_HOME}/etc/ssh_config"})
IDENTITY_FILE=$(realpath ${IDENTITY_FILE:-"${CAAR_APP_HOME}/etc/ebridge_rsa})

REPORT_PATH="$1"

HADOOP_MAX_MEMORY="-Xmx${HADOOP_MAX_MEMORY:-4G}"
TMP_REPORT_DIR="reports"

log_message() {
   echo "[$(date + "%F %T")]: $*"
}

log_message "Downloading reports from ${REPORT_PATH} to local directory: ${TMP_REPORT_DIR}"

export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} ${HADOOP_MAX_MEMORY}"

if ! hdfs dfs -test -e ${REPORT_PATH}; then
  log_message "There is no reports to send, because ${REPORT_PATH} does not exist"
  exit 0
fi

[ -e ${TMP_REPORT_DIR} ] && rm -r ${TMP_REPORT_DIR}
if hdfs dfs -stat %F ${REPORT_PATH} | grep -q 'file; then
  mkdir ${TMP_REPORT_DIR}
  TMP_REPORT_DIR=${TMP_REPORT_DIR}/
fi

if ! hdfs dfs -get ${REPORT_PATH} ${TMP_REPORT_DIR}; then
  echo "Cannot fetch reports from ${REPORT_PATH}" >&2
  exit 1
fi

cd ${TMP_REPORT_DIR}

FILE_COUNT=$(ls | wc -l)
log_message "Start sending ${FILE_COUNT} reports from local directory"

scp -F ${SSH_CONFIG_FILE} -i ${IDENTITY_FILE} -v -r . ebridge:

cd -


log_message 'All reports successfully sent'