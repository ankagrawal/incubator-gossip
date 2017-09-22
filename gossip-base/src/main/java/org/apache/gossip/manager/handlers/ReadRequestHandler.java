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

package org.apache.gossip.manager.handlers;

import java.net.URI;
import java.util.NavigableSet;

import org.apache.gossip.LocalMember;
import org.apache.gossip.Member;
import org.apache.gossip.manager.GossipCore;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.model.Base;
import org.apache.gossip.udp.UdpReadRequest;
import org.apache.gossip.udp.UdpReadWriteResponse;
import org.apache.log4j.Logger;

public class ReadRequestHandler implements MessageHandler {
	public static final Logger LOGGER = Logger.getLogger(ReadRequestHandler.class);

	private URI getUriFromId(String id, GossipManager gossipManager) {
		NavigableSet<LocalMember> members = gossipManager.getMembers().keySet();
		for(Member member : members) {
			if(member.getId().equals(id))
				return member.getUri();
		}
		return null;
	}
	
	public boolean invoke(GossipCore gossipCore, GossipManager gossipManager,
			Base base) {
		UdpReadRequest request = (UdpReadRequest)base;
		Object value = gossipCore.doRead(request.getKey());
		UdpReadWriteResponse rwResponse = new UdpReadWriteResponse();
		rwResponse.setKey(request.getKey());
		rwResponse.setValue(value);
		URI uri = getUriFromId(request.getUriFrom(), gossipManager);
		if(uri == null) {
			LOGGER.error("Cant find a member with the id to send a response");
		} else {
		    gossipCore.sendOneWay(rwResponse, uri);
		}
		return true;
	}
    
}
