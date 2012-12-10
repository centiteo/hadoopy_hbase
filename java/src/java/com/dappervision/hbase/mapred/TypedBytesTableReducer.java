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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.typedbytes.TypedBytesWritable;

public class TypedBytesTableReducer extends MapReduceBase implements
		Reducer<Text, Text, TypedBytesWritable, TypedBytesWritable> {

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<TypedBytesWritable, TypedBytesWritable> outputCollector,
			Reporter arg3) throws IOException {
		byte[] keyBytes = key.getBytes();
		TypedBytesWritable keyWritable = new TypedBytesWritable();
		TypedBytesWritable valueWritable = new TypedBytesWritable();
		keyWritable.setValue(new Buffer(keyBytes));
		
		//merge the column family and qualifier
		HashMap<String, HashMap<String, String>> cfMap = new HashMap<String, HashMap<String, String>>();
		while(values.hasNext()){
			Text value = values.next();
			String strVal = value.toString();
			//Separate column family with comma (:)
			//Separate the qualifier and value with equity
			String[] cf_qual_val_parts = strVal.split(":");
			String cf = cf_qual_val_parts[0];
			String qual_val = cf_qual_val_parts[1];
			String[] qual_val_parts = qual_val.split("=");
			String qual = qual_val_parts[0];
			String val = qual_val_parts[1];
			
			if(cfMap.get(cf)!=null){
				HashMap<String, String> qualMap = cfMap.get(cf);
				if(qualMap==null){
					qualMap = new HashMap<String, String>();
				}
				qualMap.put(qual, val); // the duplicated key will be replaced, if using Buffer, we should do it ourselves
			}else{
				HashMap<String, String> qualMap = new HashMap<String, String>();
				qualMap.put(qual, val);
				cfMap.put(cf, qualMap);
			}
		}
		
		HashMap<Buffer, HashMap<Buffer, Buffer>> bufMap= 
				new HashMap<Buffer, HashMap<Buffer, Buffer>>();
		Set<Entry<String, HashMap<String, String>>> entrySet = cfMap.entrySet();
		for(Entry<String, HashMap<String, String>> entry : entrySet){
			HashMap<String, String> qualValMap = entry.getValue();
			
			HashMap<Buffer, Buffer> qualValBufMap = new HashMap<Buffer, Buffer>();
			for(Entry<String, String> qualValEntry : qualValMap.entrySet()){
				qualValBufMap.put(new Buffer(qualValEntry.getKey().getBytes()), 
						new Buffer(qualValEntry.getValue().getBytes()));
			}
			
			bufMap.put(new Buffer(entry.getKey().getBytes()), qualValBufMap);
		}
		valueWritable.setValue(bufMap);
		
		outputCollector.collect(keyWritable, valueWritable);
	}

}