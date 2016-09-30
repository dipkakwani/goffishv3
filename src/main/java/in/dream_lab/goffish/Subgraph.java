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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;

public abstract class Subgraph {
  private static final long INITIALISATION_SUPERSTEPS = 3;
  long subgraphID;
  private List<Vertex> _vertices;
  private Map<Long, Vertex> _verticesID;
  private List<Vertex> _localVertices;
  private List<Vertex> _remoteVertices;
  private List<Edge> _edges;
  private int partitionID;
  private boolean voteToHalt;
  BSPPeer<LongWritable, Text, LongWritable, LongWritable, Text> peer;
  
  Subgraph(long subgraphID,
      BSPPeer<LongWritable, Text, LongWritable, LongWritable, Text> peer) {
    this.subgraphID = subgraphID;
    this.partitionID = peer.getPeerIndex();
    this.peer = peer;
    _vertices = new ArrayList<Vertex>();
    _localVertices = new ArrayList<Vertex>();
    _remoteVertices = new ArrayList<Vertex>();
    _verticesID = new HashMap<Long, Vertex>();
    _edges = new ArrayList<Edge>();
    voteToHalt = false;
  }

  void addVertex(Vertex v) {
    _vertices.add(v);
    _verticesID.put(v.getVertexID(), v);
  }
  
  Vertex getVertexByID(long vertexID) {
    return _verticesID.get(vertexID);
  }
  
  void addLocalVertex(Vertex v) {
    _localVertices.add(v);
    _verticesID.put(v.getVertexID(), v);
  }
  
  void addRemoteVertex(Vertex v) {
    _remoteVertices.add(v);
    _verticesID.put(v.getVertexID(), v);
  }
  
  void addEdge(Edge e) {
    _edges.add(e);
  }

  long getSubgraphID() {
    return subgraphID;
  }

  long vertexCount() {
    return _vertices.size();
  }
  
  long localVertexCount() {
    return _localVertices.size();
  }

  void voteToHalt() {
    voteToHalt = true;
  }

  boolean hasVotedToHalt() {
    return voteToHalt;
  }

  List<Vertex> getVertices() {
    return _vertices;
  }
  
  List<Vertex> getLocalVertices() {
    return _localVertices;
  }

  long getSuperStep() {
    return peer.getSuperstepCount()-INITIALISATION_SUPERSTEPS;
  }

  int getPartitionID() {
    return partitionID;
  }

  abstract void compute(List<Text> messages);
}
