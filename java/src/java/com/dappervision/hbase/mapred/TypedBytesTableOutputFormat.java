/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dappervision.hbase.mapred;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.typedbytes.Type;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TypedBytesTableOutputFormat
 */
public class TypedBytesTableOutputFormat implements OutputFormat<TypedBytesWritable, TypedBytesWritable>{
	private static final Logger LOG = 
			LoggerFactory.getLogger(TypedBytesTableOutputFormat.class);
	public static final String OUTPUT_TABLE = "hbase.mapred.outputtable";
	
	// if want to put record to another hbase cluster, you should specify the quorum and quorum port
	// in the format of <server1>,<server2>,<server3>:2181:/hbase
	public static final String QUORUM_ADDRESS = "hbase.mapred.output.quorum";
	
	public static final String QUORUM_PORT = "hbase.mapred.output.quorum.port";
	
	public static final String REGION_SERVER_CLASS = "hbase.mapred.output.rs.class";
	
	public static final String REGIOn_SERVER_IMPL = "hbase.mapred.output.rs.impl";
	
	private Configuration conf = null;
	
	private HTable table;
	
	protected static class TableRecordWriter
		implements RecordWriter<TypedBytesWritable, TypedBytesWritable> {
		private HTable table;
		public TableRecordWriter(HTable table){
			this.table = table;
		}
		
		public void write(TypedBytesWritable key, TypedBytesWritable value) throws IOException {
			//key is the table rowkey
			Type kt = key.getType();
			if(kt!=Type.BYTES){
				throw new IOException("Not expected type: '" + kt.name() + "' for key");
			}
			Put put = new Put(((Buffer)key.getValue()).get());
			//value is a map of columnfamily
			Type type = value.getType();
			if(type!=Type.MAP){
				throw new IOException("Not supported type: " + type.name());
			}
			
			// the value should be a type of Map
			Map cfMap = (Map)value.getValue();
			Set<Buffer> cfSet = (Set<Buffer>)cfMap.keySet();

			for(Buffer buffer : cfSet){
				Object valMap = cfMap.get(buffer);
				if(!(valMap instanceof Map)){
					throw new IOException("Unexpected type for qualifier set");
				}
				
				Set<Buffer> qualSet = ((Map)valMap).keySet();
				for(Buffer qualBuf : qualSet){
					Buffer valBuf = (Buffer)((Map)valMap).get(qualBuf);
					put.add(buffer.get(), qualBuf.get(), valBuf.get());
				}
			}
			
			LOG.info("Put record '" + new String(((Buffer)(key.getValue())).get()) + "'");
			this.table.put(put);
		}

		@Override
		public void close(Reporter arg0) throws IOException {
			this.table.close();
		}
	}

	@Override
	public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
			throws IOException {
		
	}

	@Override
	public RecordWriter<TypedBytesWritable, TypedBytesWritable> getRecordWriter(
			FileSystem arg0, JobConf jobConf, String arg2, Progressable arg3)
			throws IOException {
		this.conf = HBaseConfiguration.addHbaseResources(jobConf);
		LOG.debug("Zookeeper address: " + conf.get("hbase.zookeeper.quorum"));
		String tableName = this.conf.get(OUTPUT_TABLE);
		if(tableName == null || tableName.length()<=0){
			throw new IllegalArgumentException("Must specify the table name");
		}
		String address = this.conf.get(QUORUM_ADDRESS);
		int zkClientPort = conf.getInt(QUORUM_PORT, 0);
		String serverClass = this.conf.get(REGION_SERVER_CLASS);
		String serverImpl = this.conf.get(REGIOn_SERVER_IMPL);
		try{
			if(address != null) {
				ZKUtil.applyClusterKeyToConf(this.conf, address);
			}
			if(serverClass != null){
				this.conf.set(HConstants.REGION_SERVER_CLASS, serverClass);
				this.conf.set(HConstants.REGION_SERVER_IMPL, serverImpl);
			}
			
			if(zkClientPort != 0){
				conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort);
			}
			
			this.table = new HTable(this.conf, tableName);
			this.table.setAutoFlush(false);
			LOG.info("Created table instance for " + tableName);
		} catch (IOException e){
			LOG.error("", e);
			throw new RuntimeException(e);
		}
		
		return new TableRecordWriter(this.table);
	}

}
