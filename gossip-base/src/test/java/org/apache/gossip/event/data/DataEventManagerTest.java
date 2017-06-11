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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

// TODO: 6/11/17 This class should change after adding mock framework for the project
public class DataEventManagerTest {

    private static Semaphore semaphore;
    private String receivedNodeId;
    private String receivedKey;
    private Object receivedNewValue;
    private Object receivedOldValue;

    @BeforeClass
    public static void setup() {
        semaphore = new Semaphore(0);
    }

    @Test
    public void perNodeDataEventHandlerAddRemoveTest() {
        DataEventManager eventManager = new DataEventManager();

        UpdateNodeDataEventHandler nodeDataEventHandler = new UpdateNodeDataEventHandler() {
            @Override
            public void onUpdate(String nodeId, String key, Object oldValue, Object newValue) {

            }

            @Override
            public List<String> getNodeDataListeningKeys() {
                return null;
            }
        };
        eventManager.registerPerNodeDataSubscriber(nodeDataEventHandler);
        Assert.assertEquals(1, eventManager.getPerNodeSubscribers());
        eventManager.unregisterPerNodeDataSubscriber(nodeDataEventHandler);
        Assert.assertEquals(0, eventManager.getPerNodeSubscribers());
    }

    // Test whether the per node data events are fired for matching key
    @Test
    public void perNodeDataEventHandlerPositiveTest() throws InterruptedException {
        DataEventManager eventManager = new DataEventManager();
        resetData();
        // TODO: 6/11/17 replace with mock framework
        // A new subscriber "Juliet" is like to notified when per node data change for the key "Romeo"
        UpdateNodeDataEventHandler juliet = new UpdateNodeDataEventHandler() {
            @Override
            public void onUpdate(String nodeId, String key, Object oldValue, Object newValue) {
                receivedNodeId = nodeId;
                receivedKey = key;
                receivedNewValue = newValue;
                receivedOldValue = oldValue;
                semaphore.release();
            }

            @Override
            public List<String> getNodeDataListeningKeys() {
                return Collections.singletonList("Romeo");
            }
        };
        // Juliet register with eventManager
        eventManager.registerPerNodeDataSubscriber(juliet);
        // Romeo is going to sleep after having dinner
        eventManager.notifyPerNodeData("Montague", "Romeo", "sleeping", "eating");

        // Juliet should notified
        semaphore.tryAcquire(2, TimeUnit.SECONDS);
        Assert.assertEquals("Montague", receivedNodeId);
        Assert.assertEquals("Romeo", receivedKey);
        Assert.assertEquals("sleeping", receivedNewValue);
        Assert.assertEquals("eating", receivedOldValue);

        eventManager.unregisterPerNodeDataSubscriber(juliet);
    }

    // Per node data events should not fired for keys that are not interested by the handler
    @Test
    public void perNodeDataEventHandlerNegativeTest() throws InterruptedException {
        DataEventManager eventManager = new DataEventManager();
        resetData();
        // A new subscriber "Juliet" is like to notified when per node data change for the key "Romeo"
        UpdateNodeDataEventHandler juliet = new UpdateNodeDataEventHandler() {
            @Override
            public void onUpdate(String nodeId, String key, Object oldValue, Object newValue) {
                receivedNodeId = nodeId;
                receivedKey = key;
                receivedNewValue = newValue;
                receivedOldValue = oldValue;
                semaphore.release();
            }

            @Override
            public List<String> getNodeDataListeningKeys() {
                return Collections.singletonList("Romeo");
            }
        };
        // Juliet register with eventManager
        eventManager.registerPerNodeDataSubscriber(juliet);
        // Paris is thinking after having dinner
        eventManager.notifyPerNodeData("Verona", "Paris", "thinking", "eating");

        // Juliet should not get notified
        semaphore.tryAcquire(2, TimeUnit.SECONDS);
        Assert.assertEquals(null, receivedNodeId);
        Assert.assertEquals(null, receivedKey);
        Assert.assertEquals(null, receivedNewValue);
        Assert.assertEquals(null, receivedOldValue);

        eventManager.unregisterPerNodeDataSubscriber(juliet);
    }

    @Test
    public void sharedDataEventHandlerAddRemoveTest() {
        DataEventManager eventManager = new DataEventManager();

        UpdateSharedDataEventHandler sharedDataEventHandler = new UpdateSharedDataEventHandler() {
            @Override
            public void onUpdate(String key, Object oldValue, Object newValue) {

            }

            @Override
            public List<String> getSharedDataListeningKeys() {
                return null;
            }
        };
        eventManager.registerSharedDataSubscriber(sharedDataEventHandler);
        Assert.assertEquals(1, eventManager.getSharedDataSubscribers());
        eventManager.unregisterSharedDataSubscriber(sharedDataEventHandler);
        Assert.assertEquals(0, eventManager.getSharedDataSubscribers());

    }

    // Test whether the shared data events are fired for matching key
    @Test
    public void sharedDataEventHandlerPositiveTest() throws InterruptedException {
        DataEventManager eventManager = new DataEventManager();
        resetData();

        // A new subscriber "Alice" is like to notified when shared data change for the key "technology"
        UpdateSharedDataEventHandler alice = new UpdateSharedDataEventHandler() {
            @Override
            public void onUpdate(String key, Object oldValue, Object newValue) {
                receivedKey = key;
                receivedNewValue = newValue;
                receivedOldValue = oldValue;
                semaphore.release();
            }

            @Override
            public List<String> getSharedDataListeningKeys() {
                return Collections.singletonList("technology");
            }
        };
        // Alice register with eventManager
        eventManager.registerSharedDataSubscriber(alice);

        // technology key get changed
        eventManager.notifySharedData("technology", "Java has lambda", "Java is fast");

        // Alice should notified
        semaphore.tryAcquire(2, TimeUnit.SECONDS);
        Assert.assertEquals("technology", receivedKey);
        Assert.assertEquals("Java has lambda", receivedNewValue);
        Assert.assertEquals("Java is fast", receivedOldValue);

        eventManager.unregisterSharedDataSubscriber(alice);
    }

    // Shared data events should not fired for keys that are not interested by the handler
    @Test
    public void sharedDataEventHandlerNegativeTest() throws InterruptedException {
        DataEventManager eventManager = new DataEventManager();
        resetData();

        // A new subscriber "Alice" is like to notified when shared data change for the key "technology"
        UpdateSharedDataEventHandler alice = new UpdateSharedDataEventHandler() {
            @Override
            public void onUpdate(String key, Object oldValue, Object newValue) {
                receivedKey = key;
                receivedNewValue = newValue;
                receivedOldValue = oldValue;
                semaphore.release();
            }

            @Override
            public List<String> getSharedDataListeningKeys() {
                return Collections.singletonList("technology");
            }
        };
        // Alice register with eventManager
        eventManager.registerSharedDataSubscriber(alice);

        // art key get changed
        eventManager.notifySharedData("art", "new drama", "new drawing");

        // Alice should not notified
        semaphore.tryAcquire(2, TimeUnit.SECONDS);
        Assert.assertEquals(null, receivedKey);
        Assert.assertEquals(null, receivedNewValue);
        Assert.assertEquals(null, receivedOldValue);

        eventManager.unregisterSharedDataSubscriber(alice);

    }

    private void resetData() {
        receivedNodeId = null;
        receivedKey = null;
        receivedNewValue = null;
        receivedOldValue = null;
    }


}
