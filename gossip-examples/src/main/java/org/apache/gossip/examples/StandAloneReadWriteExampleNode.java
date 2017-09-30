/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gossip.examples;

import java.io.IOException;

import org.apache.gossip.consistency.Consistency;
import org.apache.gossip.consistency.ConsistencyLevel;
import org.apache.gossip.consistency.LinearTargets;
import org.apache.gossip.consistency.MajorityResponseMerger;
import org.apache.gossip.consistency.NearestTargets;
import org.apache.gossip.consistency.OperationTargets;
import org.apache.gossip.consistency.ResponseMerger;
import org.apache.gossip.manager.GossipManager;

public class StandAloneReadWriteExampleNode extends StandAloneExampleBase {
	private static boolean WILL_READ = false;
	private String jsonFile;
	private String id;

	public static void main(String[] args) throws InterruptedException, IOException {
		StandAloneReadWriteExampleNode example = null;
		try {
			example = new StandAloneReadWriteExampleNode(args);
		} catch (Exception e) {
			System.out.println(e.toString());
			System.exit(1);
		}
		example.exec(WILL_READ);
		example.runClient();
	}

	String[] checkArgsForFile(String[] args) throws Exception {
		int pos = 0;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("--file")) {
				i = i + 1;
				if(i >= args.length)
					throw new Exception("Incomplete argument --file");
				jsonFile = args[i];
			} else {
				args[pos++] = args[i];
			}
		}
		if(jsonFile == null)
			throw new Exception("no file argument provided for kvstore");
		return args;
	}

	public StandAloneReadWriteExampleNode(String[] args) throws Exception {
		jsonFile = null;
		args = super.checkArgsForClearFlag(args);
		args = checkArgsForFile(args);
		id = args[1];
		super.initGossipManager(args);
	}

	public void testReadWrite(GossipManager gossipManager) {
		OperationTargets targets = new LinearTargets(2);
		Consistency con = new Consistency(ConsistencyLevel.N, null);
		con.addParameter("n", new Integer(1));
		ResponseMerger merger = new MajorityResponseMerger();
		System.out.println("DOING READ NOW");
		Object o = gossipManager.read("a", targets, con, merger);
		System.out.println("RECIEVED READ DATA" + o.toString());
		gossipManager.write("a", 99, targets, con, merger);
		System.out.println("DATA IS WRITTEN");
		Object o1 = gossipManager.read("a", targets, con, merger);
		System.out.println("RECIEVED READ DATA" + o1.toString());
		System.out.println(o1.toString());
		Integer i1 = (Integer)o;
		Integer i2 = (Integer)o1;
		if(i1 ==i2) {
			System.out.println("Test successful!");
		} else {
			System.out.println("Test failed!");
		}
	}
	
	void runClient() {
		JsonBackedKVStore kvStore = new JsonBackedKVStore(jsonFile);
		KVStoreReadHandler readHandler = new KVStoreReadHandler(kvStore);
		KVStoreWriteHandler writeHandler = new KVStoreWriteHandler(kvStore);
		GossipManager gossipManager = this.getGossipManager();
		gossipManager.registerDataReadHandler(readHandler);
		gossipManager.registerDataWriteHandler(writeHandler);
		while(gossipManager.getLiveMembers().size() < 1) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("id is" + id);
		if(!id.equals("1")) {
			for(;;) {
				try {
					Thread.sleep(960000);
					//TODO: get a message to kill this thread once the tests are done
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} else {
			testReadWrite(gossipManager);
			gossipManager.shutdown();
		}
	}

	@Override
	void printValues(GossipManager gossipService) {
	}
}
