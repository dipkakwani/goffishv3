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
import java.util.List;

public class Vertex {
  private List<Edge> _adjList;
  private long vertexID;
  private long subgraphID;
  private long remoteSubgraphID;
  private int partitionID;
  
  Vertex(long ID, int partitionID) {
    vertexID = ID;
    this.partitionID = partitionID;
    _adjList = new ArrayList<Edge>();
    remoteSubgraphID = -1;
  }
  
  Vertex(long ID) {
    vertexID = ID;
    this.partitionID = -1;
    _adjList = new ArrayList<Edge>();
    remoteSubgraphID = -1;
  }
  
  
  void addEdge(Vertex destination) {
    Edge e = new Edge(this, destination);
    _adjList.add(e);
  }
  
  void addEdge(Edge e) {
    _adjList.add(e);
  }
  
  long getVertexID() {
    return vertexID;
  }
  
  void setSubgraphID(long ID) {
    subgraphID = ID;
  }
  
  void setRemoteSubgraphID(long ID) {
    remoteSubgraphID = ID;
  }
  
  boolean isRemote() {
    return (subgraphID == -1);
  }
  
  List<Edge> outEdges() {
    return _adjList;
  }
  
  int getPartitionID() {
    return partitionID;
  }
  
  long getSubgraphID() {
    return subgraphID;
  }
}
