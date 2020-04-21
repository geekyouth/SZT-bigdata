# HIVE_AUX_JARS_PATH={{HIVE_AUX_JARS_PATH}}
# JAVA_LIBRARY_PATH={{JAVA_LIBRARY_PATH}}
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-"$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"}"
HBASE_HIVE_DEFAULT_JAR=$(find /usr/lib/hive/lib -name hive-hbase-handler-*.jar 2> /dev/null | tail -n 1),$(sed "s: :,:g" <<< $(find /usr/lib/hbase -regextype posix-egrep -regex "/usr/lib/hbase/(hbase|hbase-client|hbase-server|hbase-protocol|hbase-common|hbase-hadoop-compat|hbase-hadoop2-compat|(lib/htrace-core.*)).jar" 2> /dev/null))
HIVE_AUX_JARS_PATH=$([[ -n $HIVE_AUX_JARS_PATH ]] && echo $HIVE_AUX_JARS_PATH,)$( ([[ ! '{{HIVE_HBASE_JAR}}' =~ HIVE_HBASE_JAR ]] &&  echo {{HIVE_HBASE_JAR}}) || echo ${HBASE_HIVE_DEFAULT_JAR:-}),$(find /usr/share/java/mysql-connector-java.jar 2> /dev/null),$(find /usr/share/cmf/lib/postgresql-*jdbc*.jar 2> /dev/null | tail -n 1),$(find /usr/share/java/oracle-connector-java.jar 2> /dev/null)
export HIVE_AUX_JARS_PATH=$(echo $HIVE_AUX_JARS_PATH | sed 's/,,*/,/g' | sed 's/^,//' | sed 's/,$//')
export HIVE_OPTS="${HIVE_OPTS} --hiveconf hive.query.redaction.rules=${HIVE_CONF_DIR}/redaction-rules.json --hiveconf hive.exec.query.redactor.hooks=org.cloudera.hadoop.hive.ql.hooks.QueryRedactor"
export HADOOP_CLIENT_OPTS="-Xmx2147483648 -Djava.net.preferIPv4Stack=true $HADOOP_CLIENT_OPTS"
