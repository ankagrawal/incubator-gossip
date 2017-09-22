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
  private ReadRequestEventHandler readRequestEventHandler;
  private final BlockingQueue<Runnable> readRequestHandlerQueue;
  private final ExecutorService readRequestEventExecutor;
  private WriteRequestEventHandler writeRequestEventHandler;
  private final BlockingQueue<Runnable> writeRequestHandlerQueue;
  private final ExecutorService writeRequestEventExecutor;
  
  public DataEventManager(MetricRegistry metrics) {
    perNodeDataHandlers = new CopyOnWriteArrayList<>();
    perNodeDataHandlerQueue = new ArrayBlockingQueue<>(DataEventConstants.PER_NODE_DATA_QUEUE_SIZE);
    perNodeDataEventExecutor = new ThreadPoolExecutor(
            DataEventConstants.PER_NODE_DATA_CORE_POOL_SIZE,
            DataEventConstants.PER_NODE_DATA_MAX_POOL_SIZE,
            DataEventConstants.PER_NODE_DATA_KEEP_ALIVE_TIME_SECONDS, TimeUnit.SECONDS,
            perNodeDataHandlerQueue, new ThreadPoolExecutor.DiscardOldestPolicy());
    
    sharedDataHandlers = new CopyOnWriteArrayList<>();
    sharedDataHandlerQueue = new ArrayBlockingQueue<>(DataEventConstants.SHARED_DATA_QUEUE_SIZE);
    sharedDataEventExecutor = new ThreadPoolExecutor(DataEventConstants.SHARED_DATA_CORE_POOL_SIZE,
            DataEventConstants.SHARED_DATA_MAX_POOL_SIZE,
            DataEventConstants.SHARED_DATA_KEEP_ALIVE_TIME_SECONDS, TimeUnit.SECONDS,
            sharedDataHandlerQueue, new ThreadPoolExecutor.DiscardOldestPolicy());

    readRequestEventHandler = null; 
    readRequestHandlerQueue = new ArrayBlockingQueue<>(DataEventConstants.READ_REQUEST_NOTIFIER_QUEUE_SIZE);
    readRequestEventExecutor = new ThreadPoolExecutor(DataEventConstants.READ_REQUEST_NOTIFIER_CORE_POOL_SIZE,
            DataEventConstants.READ_REQUEST_NOTIFIER_MAX_POOL_SIZE,
            DataEventConstants.READ_REQUEST_NOTIFIER_KEEP_ALIVE_TIME_SECONDS, TimeUnit.SECONDS,
            readRequestHandlerQueue, new ThreadPoolExecutor.DiscardOldestPolicy());

    writeRequestEventHandler = null; 
    writeRequestHandlerQueue = new ArrayBlockingQueue<>(DataEventConstants.WRITE_REQUEST_NOTIFIER_QUEUE_SIZE);
    writeRequestEventExecutor = new ThreadPoolExecutor(DataEventConstants.WRITE_REQUEST_NOTIFIER_CORE_POOL_SIZE,
            DataEventConstants.WRITE_REQUEST_NOTIFIER_MAX_POOL_SIZE,
            DataEventConstants.WRITE_REQUEST_NOTIFIER_KEEP_ALIVE_TIME_SECONDS, TimeUnit.SECONDS,
            readRequestHandlerQueue, new ThreadPoolExecutor.DiscardOldestPolicy());

    metrics.register(DataEventConstants.PER_NODE_DATA_SUBSCRIBERS_SIZE,
            (Gauge<Integer>) () -> perNodeDataHandlers.size());
    metrics.register(DataEventConstants.PER_NODE_DATA_SUBSCRIBERS_QUEUE_SIZE,
            (Gauge<Integer>) () -> perNodeDataHandlerQueue.size());
    metrics.register(DataEventConstants.SHARED_DATA_SUBSCRIBERS_SIZE,
            (Gauge<Integer>) () -> sharedDataHandlers.size());
    metrics.register(DataEventConstants.SHARED_DATA_SUBSCRIBERS_QUEUE_SIZE,
            (Gauge<Integer>) () -> sharedDataHandlerQueue.size());
    
  }
  
  public void notifySharedData(final String key, final Object newValue, final Object oldValue) {
    sharedDataHandlers.forEach(handler -> sharedDataEventExecutor
            .execute(() -> handler.onUpdate(key, oldValue, newValue)));
  }
  
  public void notifyPerNodeData(final String nodeId, final String key, final Object newValue,
          final Object oldValue) {
    perNodeDataHandlers.forEach(handler -> perNodeDataEventExecutor
            .execute(() -> handler.onUpdate(nodeId, key, oldValue, newValue)));
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

  public Object notifyReadRequest(final String key) {
	  return readRequestEventHandler.doRead(key);
  }
  
  public void registerReadRequestSubscriber(ReadRequestEventHandler handler) {
    readRequestEventHandler = handler;
  }
  
  public void unregisterReadRequestSubscriber(ReadRequestEventHandler handler) {
	readRequestEventHandler = null;
  }

  public boolean notifyWriteRequest(final String key, final Object value) {
	return writeRequestEventHandler.doWrite(key, value);
  }
  
  public void registerWriteRequestSubscriber(WriteRequestEventHandler handler) {
    writeRequestEventHandler = handler;
  }
  
  public void unregisterWriteRequestSubscriber(WriteRequestEventHandler handler) {
	  writeRequestEventHandler = null;
  }
}
