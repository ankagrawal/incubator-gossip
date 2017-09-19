package org.apache.gossip.consistency;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.gossip.LocalMember;

/**
 * Order live nodes (including oneself) based on their distance. Each node has a latitude
 * and a longitude. The distance between two nodes is the euclidean distance between their
 * co-ordinates.
 */
public class NearestTargets implements OperationTargets {
	private int numberOfReplicas;

	public NearestTargets(int numberOfReplicas) {
		this.numberOfReplicas = numberOfReplicas;
	}

	private List<LocalMember> getNNearestNodes(LocalMember me, List<LocalMember> living) {
		int n = numberOfReplicas;
		Map<Double, LocalMember> map = new TreeMap<Double, LocalMember>();
		List<LocalMember> nearestNodes = new ArrayList<LocalMember>();
		for(int i = 0; i < living.size(); i++) {
			Map<String, String> props = living.get(i).getProperties();
			double x1 = Double.parseDouble(me.getProperties().get("latitude"));
			double y1 = Double.parseDouble(me.getProperties().get("longitude"));
			double x2 = Double.parseDouble(props.get("latitude"));
			double y2 = Double.parseDouble(props.get("longitude"));
			double dist = Math.sqrt(((x1 - x2) * (x1 - x2)) + ((y1 - y2) * (y1 - y2)));
			map.put(new Double(dist), living.get(i));
		}
		for(Map.Entry<Double,LocalMember> entry : map.entrySet()) {
	        nearestNodes.add(entry.getValue());
	        n--;
	        if (n == 0) {
	        	break;
	        }
	    }
		return nearestNodes;
	}

	private boolean isValidNode(LocalMember node) {
		if(node.getProperties().containsKey("longitude") && node.getProperties().containsKey("latitude"))
			return true;
		return false;
	}
	
	private boolean enoughValidNodes(List<LocalMember> members) {
		if(members.size() < this.numberOfReplicas)
			return false;
		return true;
	}
	
	private List<LocalMember> getLivingNodesWithCoordinates(List<LocalMember> living, LocalMember me) {
		List<LocalMember> membersWithCoordinates = new ArrayList<LocalMember>();
		for(int i = 0; i < living.size(); i++) {
			if(isValidNode(living.get(i))) {
				membersWithCoordinates.add(living.get(i));
			}
		}
		return membersWithCoordinates;
	}

	public List<LocalMember> generateTargets(String key, LocalMember me,
			List<LocalMember> living, List<LocalMember> dead) {
		if(!isValidNode(me))
			throw new RuntimeException("Current node " + me.toString() + "doesnt have longitude and latitude properties");
		List<LocalMember> membersWithCoordinates = getLivingNodesWithCoordinates(living, me);
		membersWithCoordinates.add(me);
		if (!enoughValidNodes(membersWithCoordinates)) {
			throw new RuntimeException("Not enough live nodes with longitude and latitude properties");
		}
		return getNNearestNodes(me, living);
	}

}
