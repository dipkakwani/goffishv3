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

public interface IEdge<E extends Writable, J extends Writable>{
  //private I _source;
  //private I _sink; Seperate interace
  private E _value;
  private J _id;
  
  Edge(Vertex<V, E> u, Vertex<V, E> v) {
    _source = u;
    _sink = v;
  }
  
  Vertex<V, E> getSource() {
    return _source;
  }
  
  Vertex<V, E> getSink() {
    return _sink;
  }
  
  public E getValue() {
    return _value;
  }
  
  public void setValue(E val) {
    _value = val;
  }
}
