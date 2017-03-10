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

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Superstep;

import in.dream_lab.goffish.api.IMessage;

public class PhaseCompute<S extends Writable, V extends Writable, E extends Writable, M extends Writable, I extends Writable, J extends Writable, K extends Writable> extends Superstep<Writable, Writable, Writable, Writable, Message<K, M>>{

  private SubgraphCompute<S, V, E, M, I, J, K> _subgraphComputeRunner;
  private Collection<IMessage<K,M>> _messagesToSubgraph;
  
  PhaseCompute(SubgraphCompute<S, V, E, M, I, J, K> phase, Collection<IMessage<K,M>> messages) {
    this._subgraphComputeRunner = phase;
    this._messagesToSubgraph = messages;
  }
  
  void setComputeRunner(SubgraphCompute<S, V, E, M, I, J, K> phase) {
    this._subgraphComputeRunner = phase;
  }
  
  void setMessages(Collection<IMessage<K,M>> messages) {
    this._messagesToSubgraph = messages;
  }
  
  @Override
  protected void compute(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer)
      throws IOException {
    if (!(_messagesToSubgraph.size() == 0 && _subgraphComputeRunner.hasVotedToHalt())) {
      _subgraphComputeRunner.compute(_messagesToSubgraph);
    }
    
  }

}
