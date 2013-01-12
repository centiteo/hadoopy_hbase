echo "Copying libs into test directory"
HADOOP_PATH="/usr/lib/hadoop"
HBASE_PATH="/usr/lib/hbase"
ZOOKEEPER_PATH="/usr/lib/zookeeper"
mkdir -p ./lib/
mkdir -p ./test/conf

/bin/cp ${HBASE_PATH}/lib/commons-logging-* ./lib/
/bin/cp ${HBASE_PATH}/lib/commons-configuration-* ./lib/
/bin/cp ${HBASE_PATH}/lib/commons-lang-* ./lib/
/bin/cp ${HBASE_PATH}/lib/commons-codec-1.4.jar ./lib/
/bin/cp ${HBASE_PATH}/lib/protobuf-java-* ./lib/
/bin/cp ${HBASE_PATH}/lib/guava-* ./lib/
/bin/cp ${HBASE_PATH}/lib/log4j-* ./lib/
/bin/cp ${HBASE_PATH}/lib/slf4j-*.jar ./lib/
/bin/cp ${HBASE_PATH}/hbase-* ./lib/
/bin/cp ${ZOOKEEPER_PATH}/zookeeper-* ./lib
/bin/cp ${HADOOP_PATH}/hadoop-core-*.jar ./lib/

/bin/cp ${HADOOP_PATH}/conf/hdfs-site.xml ./test/conf
/bin/cp ${HBASE_PATH}/conf/hbase-site.xml ./test/conf

echo "Run test code"
ant run-test

