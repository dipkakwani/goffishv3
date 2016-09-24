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

public class Partition {
  private int partitionID;
  private List<Subgraph> _subgraphs;
  private Map<Long, Subgraph> _subgraphMap;
  
  Partition(int ID) {
    partitionID = ID;
    _subgraphs = new ArrayList<Subgraph>();
    _subgraphMap = new HashMap<Long, Subgraph>();
  }
  
  int getPartitionID() {
    return partitionID;
  }
  
  void addSubgraph(Subgraph subgraph) {
    _subgraphs.add(subgraph);
    _subgraphMap.put(subgraph.getSubgraphID(), subgraph);
  }
  
  List<Subgraph> getSubgraphs() {
    return _subgraphs;
  }
  
  Subgraph getSubgraph(long subgraphID) {
    return _subgraphMap.get(subgraphID);
  }
}
