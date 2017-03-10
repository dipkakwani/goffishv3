/*
    *      Copyright 2017 DREAM:Lab, Indian Institute of Science, Bangalore
    *
    *      Licensed under the Apache License, Version 2.0 (the "License"); you may
    *      not use this file except in compliance with the License. You may obtain
    *      a copy of the License at
    *
    *      http://www.apache.org/licenses/LICENSE-2.0
    *
    *      Unless required by applicable law or agreed to in writing, software
    *      distributed under the License is distributed on an "AS IS" BASIS,
    *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    *      See the License for the specific language governing permissions and
    *      limitations under the License.
*/

package in.dream_lab.goffish.api;

import in.dream_lab.goffish.api.ISubgraphCompute;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

public abstract class AbstractSubgraphComputation<S extends Writable, V extends Writable, E extends Writable, M extends Writable, I extends Writable, J extends Writable, K extends Writable> {
  
  private ISubgraphCompute<S, V, E, M, I, J, K> subgraphPlatformCompute;

  public long getSuperStep() {
    return subgraphPlatformCompute.getSuperstep();
  }

  public void setSubgraphPlatformCompute(ISubgraphCompute<S, V, E, M, I, J, K> subgraphPlatformCompute) {
    this.subgraphPlatformCompute = subgraphPlatformCompute;
  }

  public ISubgraph<S, V, E, I, J, K> getSubgraph() {
    return subgraphPlatformCompute.getSubgraph();
  }

  public void voteToHalt() {
    subgraphPlatformCompute.voteToHalt();
  }

  public abstract void compute(Iterable<IMessage<S, M>> messages) throws IOException;

  public void sendMessage(K subgraphId, M message) {
    subgraphPlatformCompute.sendMessage(subgraphId, message);
  }

  public void sendToNeighbors(M message) {
    subgraphPlatformCompute.sendToNeighbors(message);
  }


  void sendMessage(K subgraphID, Iterable<M> message) {
    subgraphPlatformCompute.sendMessage(subgraphID, message);
  }


  void sendToAll(Iterable<M> message) {
    subgraphPlatformCompute.sendToAll(message);
  }

  void sendToNeighbors(Iterable<M> message) {
    subgraphPlatformCompute.sendToNeighbors(message);
  }

}