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
import java.util.List;

import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IVertex;

public class Vertex<V extends Writable, E extends Writable, I extends Writable, J extends Writable> implements IVertex<V, E, I, J> {
  private List<IEdge<E, I, J>> _adjList;
  private I vertexID;
  private V _value;
  
  Vertex(I ID) {
    vertexID = ID;
    _adjList = new ArrayList<IEdge<E, I, J>>();
  }
  
  void addEdge(IEdge<E, I, J> edge) {
    _adjList.add(edge);
  }
  
  @Override
  public I getVertexID() {
    return vertexID;
  }
  
  @Override
  public boolean isRemote() {
    return false;
  }
  
  @Override
  public Collection<IEdge<E, I, J>> outEdges() {
    return _adjList;
  }

  @Override
  public V getValue() {
    return _value;
  }

  @Override
  public void setValue(V value) {
    _value = value;
  }
}
