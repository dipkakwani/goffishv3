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

import org.apache.hadoop.io.Writable;

public abstract class SubgraphCompute <S extends Writable, V extends Writable, E extends Writable, M extends Writable, I extends Writable, J extends Writable, K extends Writable> implements ISubgraphCompute<S, V, E, M, I, J, K> {
  ISubgraph<S, V, E, I, J, K> subgraph;
  long superStepCount;
  boolean voteToHalt;
  GraphJobRunner<S, V, E, M, I, J, K> runner = new GraphJobRunner<S, V, E, M, I, J, K>();
  
  @Override
  public long getSuperStep() {
    return runner.getSuperStepCount();
  }
  
  @Override
  public ISubgraph<S, V, E, I, J, K> getSubgraph() {
    return subgraph;
  }
  
  @Override
  public void voteToHalt() {
    voteToHalt = false;
  }
  
  boolean hasVotedToHalt() {
    return voteToHalt;
  }
  
  void setSubgraph(ISubgraph<S, V, E, I, J, K> subgraph) {
    this.subgraph = subgraph;
  }
  
  @Override
  public void sendMessage(K subgraphID, M message) {
    runner.sendMessage(subgraphID, message);
  }
  
  @Override
  public void sendToAll(M message) {
    runner.sendToAll(message);
  }
  
  @Override
  public void sendToNeighbors(M message) {
    runner.sendToNeighbors(subgraph, message);
  }
  

}
