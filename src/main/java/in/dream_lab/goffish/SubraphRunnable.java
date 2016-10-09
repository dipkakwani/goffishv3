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

/* FIXME: Make it actually generic. */
public abstract class SubgraphRunnable <S extends Writable, V extends Writable, E extends Writable, M extends Writable> implements SubgraphInterface<S, V, E, M>{
  private static final long INITIALISATION_SUPERSTEPS = 3;
  long subgraphID;
  private List<Vertex<V, E>> _vertices;
  private Map<Long, Vertex<V, E>> _verticesID;
  private List<Vertex<V, E>> _localVertices;
  private List<Vertex<V, E>> _remoteVertices;
  private List<Edge<V, E>> _edges;
  private int partitionID;
  private boolean voteToHalt;
  BSPPeer<LongWritable, Text, LongWritable, LongWritable, Text> peer;
  S _value;
  
  Subgraph(long subgraphID,
      BSPPeer<LongWritable, Text, LongWritable, LongWritable, Text> peer) {
    this.subgraphID = subgraphID;
    this.partitionID = peer.getPeerIndex();
    this.peer = peer;
    _vertices = new ArrayList<Vertex<V, E>>();
    _localVertices = new ArrayList<Vertex<V, E>>();
    _remoteVertices = new ArrayList<Vertex<V, E>>();
    _verticesID = new HashMap<Long, Vertex<V, E>>();
    _edges = new ArrayList<Edge<V, E>>();
    voteToHalt = false;
  }

  void addVertex(Vertex<V, E> v) {
    _vertices.add(v);
    _verticesID.put(v.getVertexID(), v);
  }
  
  public Vertex<V, E> getVertexByID(long vertexID) {
    return _verticesID.get(vertexID);
  }
  
  void addLocalVertex(Vertex<V, E> v) {
    _localVertices.add(v);
    _verticesID.put(v.getVertexID(), v);
  }
  
  void addRemoteVertex(Vertex<V, E> v) {
    _remoteVertices.add(v);
    _verticesID.put(v.getVertexID(), v);
  }
  
  void addEdge(Edge<V, E> e) {
    _edges.add(e);
  }

  public long getSubgraphID() {
    return subgraphID;
  }

  public long vertexCount() {
    return _vertices.size();
  }
  
  public long localVertexCount() {
    return _localVertices.size();
  }

  public void voteToHalt() {
    voteToHalt = true;
  }

  public boolean hasVotedToHalt() {
    return voteToHalt;
  }

  public List<Vertex<V, E>> getVertices() {
    return _vertices;
  }
  
  public List<Vertex<V, E>> getLocalVertices() {
    return _localVertices;
  }

  public long getSuperStep() {
    return peer.getSuperstepCount()-INITIALISATION_SUPERSTEPS;
  }

  public int getPartitionID() {
    return partitionID;
  }
  
  public void setValue(S value) {
    _value = value;
  }
  
  public S getValue() {
    return _value; 
  }
  //public abstract void compute(List<Text> messages);
}
