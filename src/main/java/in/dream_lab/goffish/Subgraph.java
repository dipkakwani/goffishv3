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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;

public class Subgraph <S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable> implements ISubgraph<S, V, E, I, J, K> {
  private static final long INITIALISATION_SUPERSTEPS = 3;
  K subgraphID;
  private List<IVertex<V, E, I, J>> _vertices;
  private Map<I, IVertex<V, E, I, J>> _verticesID;
  //private List<IVertex<V, E, I, J>> _localVertices;
  private List<IRemoteVertex<V, E, I, J, K>> _remoteVertices;
  private List<IEdge<E, I, J>> _edges; 
  //BSPPeer<Writable, Writable, Writable, Writable, Text> peer;
  int partitionID;
  S _value;
  private boolean votedToHalt;
  
  Subgraph(int partitionID, K subgraphID) {
    this.partitionID = partitionID;
    this.subgraphID = subgraphID;
    _vertices = new ArrayList<IVertex<V, E, I, J>>();
    //_localVertices = new ArrayList<IVertex<V, E, I, J>>();
    _remoteVertices = new ArrayList<IRemoteVertex<V, E, I, J, K>>();
    _verticesID = new HashMap<I, IVertex<V, E, I, J>>();
    _edges = new ArrayList<IEdge<E, I, J>>();
  }

  void addVertex(IVertex<V, E, I, J> v) {
    _vertices.add(v);
    System.out.println("2");
    if (v.isRemote()) {
      System.out.println("2.5");
      _remoteVertices.add((IRemoteVertex<V, E, I, J, K>)v);
    }
    System.out.println("3");
    _verticesID.put(v.getVertexID(), v);
  }
  
  @Override
  public IVertex<V, E, I, J> getVertexByID(I vertexID) {
    return _verticesID.get(vertexID);
  }
  
  void addEdge(IEdge<E, I, J> e) {
    _edges.add(e);
  }

  @Override
  public K getSubgraphID() {
    return subgraphID;
  }

  @Override
  public long vertexCount() {
    return _vertices.size();
  }
  
  @Override
  public long localVertexCount() {
    return _vertices.size() - _remoteVertices.size();
  }

  @Override
  public List<IVertex<V, E, I, J>> getVertices() {
    return _vertices;
  }
  
  /* Avoid using this function as it is inefficient. */
  @Override
  public List<IVertex<V, E, I, J>> getLocalVertices() {
    List<IVertex<V, E, I, J>> localVertices = new ArrayList<IVertex<V, E, I, J>>();
    for (IVertex<V, E, I, J> v : _vertices)
      if (!v.isRemote())
        localVertices.add(v);
    return localVertices;
  }
  
  @Override
  public void setValue(S value) {
    _value = value;
  }
  
  @Override
  public S getValue() {
    return _value; 
  }
  //public abstract void compute(List<Text> messages);

  @Override
  public List<IRemoteVertex<V, E, I, J, K>> getRemoteVertices() {
    return _remoteVertices;
  }
}
