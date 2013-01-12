echo "Copying libs into local build directory"
HADOOP_PATH="/usr/lib/hadoop"
HBASE_PATH="/usr/lib/hbase"
mkdir -p ./lib/
cp ${HBASE_PATH}/lib/commons-logging* ./lib/
cp ${HBASE_PATH}/lib/commons-codec-1.4.jar ./lib/
cp ${HBASE_PATH}/lib/slf4j-*.jar ./lib/
cp ${HBASE_PATH}/hbase-* ./lib/
cp ${HADOOP_PATH}/hadoop-core-*.jar ./lib/
cp ${HADOOP_PATH}/contrib/streaming/hadoop-streaming-*.jar ./lib/

echo "Building hadoopy_hbase.jar"
ant

