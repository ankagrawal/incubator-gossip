package org.apache.gossip.manager;

import java.util.List;

import org.apache.gossip.model.Response;

public interface ResponseMerger {
	Response merge(List<? extends Response> responses);
}
