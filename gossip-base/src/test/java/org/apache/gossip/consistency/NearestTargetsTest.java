package org.apache.gossip.consistency;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.gossip.LocalMember;
import org.junit.Assert;
import org.junit.Test;

public class NearestTargetsTest {
	
	@Test
	public void basicTestWithOneNode() {
		NearestTargets p = new NearestTargets(1);
		LocalMember me = new LocalMember() { };
		me.setId("1");
		Map<String, String> props = new HashMap<String, String>();
		props.put("latitude", "" + 1);
		props.put("longitude", "" + 2.9);
		me.setProperties(props);
		Assert.assertEquals("1", p.generateTargets("", me, new ArrayList<LocalMember>(), new ArrayList<LocalMember>()).get(0).getId());
	}
	
	@Test
	public void testWith10Nodes() {
		NearestTargets p = new NearestTargets(4);
		List<LocalMember> memList = new ArrayList<LocalMember>();
		for(int i = 1; i < 10; i++) {
			LocalMember mem = new LocalMember() { };
			mem.setId(i + "");
			Map<String, String> props = new HashMap<String, String>();
			props.put("latitude", "0");
			props.put("longitude", "" + i);
			mem.setProperties(props);
			memList.add(mem);
		}
		LocalMember me = new LocalMember() { };
		me.setId("0");
		Map<String, String> props = new HashMap<String, String>();
		props.put("latitude", "0");
		props.put("longitude", "0");
		me.setProperties(props);
		List<LocalMember> generatedTargets = p.generateTargets("", me, memList, new ArrayList<LocalMember>());
		Assert.assertEquals(4, generatedTargets.size());
		for(int i = 0; i < 4; i++) {
			Assert.assertEquals(i + "", generatedTargets.get(i).getId());
		}
	}
}