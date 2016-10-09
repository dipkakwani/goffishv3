/**
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
package in.dream_lab.goffish;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public interface IPartition<S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable>{
  
  int getPartitionID();
  
  void addSubgraph(ISubgraph<S, V, E, I, J, K> subgraph);
  
  void removeSubgraph(K subgraphID);
  
  //Return in descending order for vertices.
  Iterable<ISubgraph<S, V, E, I, J, K>> getSubgraphs();
  
  ISubgraph<S, V, E, I, J, K> getSubgraph(K subgraphID);
}
