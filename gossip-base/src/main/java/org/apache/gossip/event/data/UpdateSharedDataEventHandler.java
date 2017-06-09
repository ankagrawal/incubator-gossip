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

import java.util.List;

/**
 * Event handler interface for shared data items.
 * Classes which implement this interface get notifications when shared data get changed based on
 * the key list that class defined.
 */
public interface UpdateSharedDataEventHandler {
  /**
   * This method get called when shared data get changed.
   *
   * @param key      key of the shared data item
   * @param oldValue previous value or null if the data is discovered for the first time
   * @param newValue updated value of the data item
   */
  void onUpdate(String key, Object oldValue, Object newValue);
  
  /**
   * This method get called to retrieve the key list that implemented class should interested in.
   * The implemented class only get notifications for the key list returned by this method.
   *
   * @return interested list of shared data keys that the implementation class will like
   * to get notified
   */
  List<String> getSharedDataListeningKeys();
  
}
