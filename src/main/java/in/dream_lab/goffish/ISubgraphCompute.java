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

import java.util.Collection;
import org.apache.hadoop.io.Writable;


public interface ISubgraphCompute <S extends Writable, V extends Writable, E extends Writable, M extends Writable, I extends Writable, J extends Writable, K extends Writable> {
  
  ISubgraph<S, V, E, I, J, K> getSubgraph();
  
  void voteToHalt();

  long getSuperStep();

  void compute(Collection<IMessage<K, M>> messages);
  
  void reduce(Collection<IMessage<K, M>> messages);
  
  void sendMessage(K subgraphID, M message);
  
  void sendToVertex(I vertexID, M message);
  
  void sendToAll(M message); // auto fill subgraph ID on send or receive
  
  void sendToNeighbors(M message);
  
  /* TODO:
  void sendMessage(K subgraphID, Collection<M> message);
  
  void sendToAll(Collection<M> message);
  
  void sendToNeighbors(Collection<M> message);
   */
}
