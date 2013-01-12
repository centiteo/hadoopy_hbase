#!/bin/sh

# Clean the build staff
ant clean

# Build jar over IDH
/bin/sh build_idh.sh

# Prepare the input and output table
/bin/sh prepare.sh

# Read from HBase table and output to hdfsa
hadoop fs -rmr /user/root/test # Make sure the file is useless

hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-1.0.3-Intel.jar -libjars ./build/dist/hadoopy_hbase.jar -conf /usr/lib/hbase/conf/hbase-site.xml -D mapred.reduce.tasks=0 -D hbase.mapred.tablecolumns=f1:col1 -input /user/hbase/input -output /user/root/test -inputformat com.dappervision.hbase.mapred.TypedBytesTableInputFormat -mapper /bin/cat

# Read from hdfs and output to HBase table
hadoop fs -rmr /user/root/record.txt # Make sure the file is useless

hadoop fs -copyFromLocal ./test/data/record.txt /user/root/record.txt

hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-1.0.3-Intel.jar -libjars ./build/dist/hadoopy_hbase.jar -conf /user/lib/hbase/conf/hbase-site.xml -D hbase.mapred.outputtable=output -input /user/root/record.txt -output hbase -outputformat com.dappervision.hbase.mapred.TypedBytesTableOutputFormat -mapper /bin/cat -reducer com.dappervision.hbase.mapred.TypedBytesTableReducer
