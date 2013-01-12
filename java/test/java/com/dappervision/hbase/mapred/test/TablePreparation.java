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
package com.dappervision.hbase.mapred.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TablePreparation {

	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin.checkHBaseAvailable(config);

		// Create input table
		String inputTableName = "input";
		HBaseAdmin hbase = new HBaseAdmin(config);
		HTableDescriptor desc = new HTableDescriptor(inputTableName);
		HColumnDescriptor f1 = new HColumnDescriptor("f1".getBytes());
		desc.addFamily(f1);
		hbase.createTable(desc);

		// Put records
		HTable inputTable = new HTable(config, inputTableName);
		byte[] row1 = Bytes.toBytes("row1");
		Put put1 = new Put(row1);
		put1.add("f1".getBytes(), "col1".getBytes(), "value1".getBytes());
		inputTable.put(put1);

		byte[] row2 = Bytes.toBytes("row2");
		Put put2 = new Put(row2);
		put2.add("f1".getBytes(), "col1".getBytes(), "value2".getBytes());
		inputTable.put(put2);

		inputTable.close();

		// Create output table
		String outputTableName = "output";
		desc = new HTableDescriptor(outputTableName);
		f1 = new HColumnDescriptor("f1".getBytes());
		desc.addFamily(f1);
		hbase.createTable(desc);
	}
}
