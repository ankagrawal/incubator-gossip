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
package org.apache.gossip.event.data;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DataEventManager {

  private final List<UpdateNodeDataEventHandler> perNodeDataHandlers;
  private final BlockingQueue<Runnable> perNodeDataHandlerQueue;
  private final ExecutorService perNodeDataEventExecutor;
  private final List<UpdateSharedDataEventHandler> sharedDataHandlers;
  private final BlockingQueue<Runnable> sharedDataHandlerQueue;
  private final ExecutorService sharedDataEventExecutor;
  public static final String PER_NODE_DATA_SUBSCRIBERS_SIZE = "gossip.event.data.pernode.subscribers.size";
  public static final String PER_NODE_DATA_SUBSCRIBERS_QUEUE_SIZE = "gossip.event.data.pernode.subscribers.queue.size";
  public static final String SHARED_DATA_SUBSCRIBERS_SIZE = "gossip.event.data.shared.subscribers.size";
  public static final String SHARED_DATA_SUBSCRIBERS_QUEUE_SIZE = "gossip.event.data.shared.subscribers.queue.size";

  public DataEventManager(MetricRegistry metrics) {
    perNodeDataHandlers = new CopyOnWriteArrayList<>();
    perNodeDataHandlerQueue = new ArrayBlockingQueue<>(64);
    perNodeDataEventExecutor = new ThreadPoolExecutor(1, 30, 1, TimeUnit.SECONDS,
            perNodeDataHandlerQueue, new ThreadPoolExecutor.DiscardOldestPolicy());

    sharedDataHandlers = new CopyOnWriteArrayList<>();
    sharedDataHandlerQueue = new ArrayBlockingQueue<Runnable>(64);
    sharedDataEventExecutor = new ThreadPoolExecutor(1, 30, 1, TimeUnit.SECONDS,
            sharedDataHandlerQueue, new ThreadPoolExecutor.DiscardOldestPolicy());

    metrics.register(PER_NODE_DATA_SUBSCRIBERS_SIZE,
            (Gauge<Integer>) () -> perNodeDataHandlers.size());
    metrics.register(PER_NODE_DATA_SUBSCRIBERS_QUEUE_SIZE,
            (Gauge<Integer>) () -> perNodeDataHandlerQueue.size());
    metrics.register(SHARED_DATA_SUBSCRIBERS_SIZE,
            (Gauge<Integer>) () -> sharedDataHandlers.size());
    metrics.register(SHARED_DATA_SUBSCRIBERS_QUEUE_SIZE,
            (Gauge<Integer>) () -> sharedDataHandlerQueue.size());

  }

  public void notifySharedData(final String key, final Object newValue, final Object oldValue) {
    sharedDataHandlers.stream()
            .filter(handler -> handler.getSharedDataListeningKeys() != null && handler
                    .getSharedDataListeningKeys().contains(key)).forEach(handler -> {
      sharedDataEventExecutor.execute(() -> {
        handler.onUpdate(key, oldValue, newValue);
      });
    });
  }

  public void notifyPerNodeData(final String nodeId, final String key, final Object newValue,
          final Object oldValue) {
    perNodeDataHandlers.stream()
            .filter(handler -> handler.getNodeDataListeningKeys() != null && handler
                    .getNodeDataListeningKeys().contains(key)).forEach(handler -> {
      perNodeDataEventExecutor.execute(() -> {
        handler.onUpdate(nodeId, key, oldValue, newValue);
      });
    });
  }

  public void registerPerNodeDataSubscriber(UpdateNodeDataEventHandler handler) {
    perNodeDataHandlers.add(handler);
  }

  public void unregisterPerNodeDataSubscriber(UpdateNodeDataEventHandler handler) {
    perNodeDataHandlers.remove(handler);
  }

  public int getPerNodeSubscribersSize() {
    return perNodeDataHandlers.size();
  }

  public void registerSharedDataSubscriber(UpdateSharedDataEventHandler handler) {
    sharedDataHandlers.add(handler);
  }

  public void unregisterSharedDataSubscriber(UpdateSharedDataEventHandler handler) {
    sharedDataHandlers.remove(handler);
  }

  public int getSharedDataSubscribersSize() {
    return sharedDataHandlers.size();
  }

}
