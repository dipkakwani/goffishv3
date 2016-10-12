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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;


/* Reads graph in the adjacency list format:
 * VID PartitionID StartEdgeID Sink1 Sink2 ...*/
public class LongTextAdjacencyListReader<S extends Writable, V extends Writable, E extends Writable> 
 implements IReader <Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {
  
  Map<LongWritable, Vertex<V, E, LongWritable, LongWritable>> vertexMap;
  BSPPeer<Writable, Writable, Writable, Writable, IMessage<LongWritable>> peer;
  Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition;
  
  List<Subgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs() {
    Map<IntWritable, List<Vertex<V, E, LongWritable, LongWritable>>> partitionMap = new HashMap<IntWritable, List<Vertex<V, E, LongWritable, LongWritable>>>();
    Map<LongWritable, Vertex<V, E, LongWritable, LongWritable>> vertexMap = new HashMap<LongWritable, Vertex<V, E, LongWritable, LongWritable>>();
    List<Vertex<V, E, LongWritable, LongWritable>> verticesList = new ArrayList<Vertex<V, E, LongWritable, LongWritable>>();
    
    KeyValuePair<Writable, Writable> pair;
    long numPeers = peer.getNumPeers();
    while ((pair = peer.readNext()) != null) {
      //NOTE: Confirm that data starts from value and not from key.
      String value[] = pair.getValue().toString().split("\\s+");
      LongWritable sourceID = new LongWritable(Long.parseLong(value[0]));
      IntWritable partitionID = new IntWritable(Integer.parseInt(value[1]));
      LongWritable startEdgeID = new LongWritable(Long.parseLong(value[2]));
      List<Vertex<V, E, LongWritable, LongWritable>> partitionVertices = partitionMap.get(partitionID);
      if (partitionVertices == null) {
        partitionVertices = new ArrayList<Vertex<V, E, LongWritable, LongWritable>>();
      }
      Vertex<V, E, LongWritable, LongWritable> source = vertexMap.get(sourceID);
      if (source == null) {
        source = new Vertex<V, E, LongWritable, LongWritable>(sourceID);
        vertexMap.put(sourceID, source);
        verticesList.add(source);
      }
      partitionVertices.add(source);
      for (int i = 3; i < value.length; i++) {
        LongWritable sinkID = new LongWritable(Long.parseLong(value[i]));
        Vertex<V, E, LongWritable, LongWritable> sink = vertexMap.get(sinkID);   
        if (sink == null) {
          sink = new Vertex<V, E, LongWritable, LongWritable>(sinkID);
          vertexMap.put(sinkID, sink);
          verticesList.add(sink);
        }
        Edge<E, LongWritable, LongWritable> e = new Edge<E, LongWritable, LongWritable>(startEdgeID, sinkID);
        startEdgeID.set(startEdgeID.get() + 1);
        source.addEdge(e);
      }
    }
    // TODO: Incomplete.
    List<IVertex<V, E, LongWritable, LongWritable>> _vertices = new ArrayList<IVertex<V, E, LongWritable, LongWritable>>(); // Final list of vertices.
    //vertexMap = new HashMap<Long, IVertex<V, E, LongWritable, LongWritable>>();

    // Send vertices to their respective partitions.
    for (Map.Entry<IntWritable, List<Vertex<V, E, LongWritable, LongWritable>>> entry : partitionMap.entrySet()) {
      
      if (entry.getKey().get() != peer.getPeerIndex()) {
        StringBuilder sb = new StringBuilder();
        for (Vertex<V, E, LongWritable, LongWritable> v : entry.getValue()) {
          sb.append(v.getVertexID());
          for (IEdge<E, LongWritable, LongWritable> e : v.outEdges()) {
            Edge<E, LongWritable, LongWritable> edge = (Edge<E, LongWritable, LongWritable>)e;
            sb.append(' ').append(edge.getSink());
          }
        }
        Message<LongWritable> msg = new Message<LongWritable>(IMessage.MessageType.VERTEX, entry.getKey().get(), String.valueOf(sb).getBytes());
        //Text msg = new Text(sb.toString());
        peer.send(peer.getPeerName(entry.getKey().get()), msg);
      } else { // Belongs to this partition
        _vertices.addAll(entry.getValue());
        //vertexMap.put(v.getVertexID(), v);
      }
    }
    for (IVertex<V, E, LongWritable, LongWritable> v : _vertices) {
      System.out.println(v.getVertexID());
    }
    
    System.out.println(_vertices.size()+"=size="+vertexMap.size());

    // End of first superstep.
    peer.sync();
    
    IMessage<LongWritable> msg;
    while ((msg = peer.getCurrentMessage()) != null) {
      String msgString = msg.toString();
      String msgStringArr[] = msgString.split(",");
      long vertexID = Long.parseLong(msgStringArr[0]);
      Vertex<V, E, LongWritable, LongWritable> v = new Vertex<V, E, LongWritable, LongWritable>(vertexID, getPartitionID(vertexID));
      _vertices.add(v);
      vertexMap.put(v.getVertexID(), v);
      for (int i = 1; i < msgStringArr.length; i++) {
        long sinkID = Long.valueOf(msgStringArr[i]);
        IVertex<V, E, LongWritable, LongWritable> sink = vertexMap.get(sinkID);
        if (sink == null) {
          sink = new IVertex<V, E, LongWritable, LongWritable>(sinkID, (int)(sinkID % numPeers));
          vertexMap.put(sinkID, sink);
        }
        Edge<V, E> e = new Edge<V, E>(v, sink);
        v.addEdge(e);
      }
    }
    formSubgraphs(_vertices);

    /*
     * Ask Remote Vertices to send their subgraph IDs. Requires 2 supersteps
     * because the graph is directed
     */
    for (IVertex<V, E, LongWritable, LongWritable> v : _vertices) {
      if (v.isRemote()) {
        msg = new Text(v.getVertexID() + "," + peer.getPeerIndex());
        peer.send(peer.getPeerName(getPartitionID(v)), msg);
      }
    }

    peer.sync();

    while ((msg = peer.getCurrentMessage()) != null) {
      String msgString = msg.toString();
      String msgStringArr[] = msgString.split(",");
      Long sinkID = Long.valueOf(msgStringArr[0]);
      for (IVertex<V, E, LongWritable, LongWritable> v : _vertices) {
        if (sinkID == v.getVertexID()) {
          peer.send(peer.getPeerName(Integer.parseInt(msgStringArr[1])),
              new Text(v.getVertexID() + "," + v.getSubgraphID()));
        }
      }
    }
    
    peer.sync();
    System.out.println("Messages to all neighbours sent");
    
    while ((msg = peer.getCurrentMessage()) != null) {
      String msgString = msg.toString();
      String msgStringArr[] = msgString.split(",");
      Long sinkID = Long.parseLong(msgStringArr[0]);
      Long remoteSubgraphID = Long.parseLong(msgStringArr[1]);
      for(IVertex<V, E, LongWritable, LongWritable> v : _vertices) {
        if (v.getVertexID() == sinkID) {
          v.setRemoteSubgraphID(remoteSubgraphID);
        }
      }
    }
  }
  
  /* Forms subgraphs by finding (weakly) connected components. */
  void formSubgraphs(List<IVertex<V, E, LongWritable, LongWritable>> vertices) {
    long subgraphCount = 0;
    Set<Long> visited = new HashSet<Long>();

    for (IVertex<V, E, LongWritable, LongWritable> v : vertices) {
      if (!visited.contains(v.getVertexID())) {
        long subgraphID = subgraphCount++ | (((long) partition.getPartitionID()) << 32);
        Subgraph subgraph = new VertexCount.VrtxCnt(subgraphID, peer);
        dfs(v, visited, subgraph);
        partition.addSubgraph(subgraph);
        System.out.println("Subgraph " + subgraph.getSubgraphID() + "has "
            + subgraph.vertexCount() + "Vertices");
      }
    }
  }
  
  void dfs(IVertex<V, E, LongWritable, LongWritable> v, Set<Long> visited, Subgraph subgraph) {
    if (peer.getPeerIndex() == getPartitionID(v)) {
      v.setSubgraphID(subgraph.getSubgraphID());
      subgraph.addLocalVertex(v);
    } else {
      v.setSubgraphID(-1);
      subgraph.addRemoteVertex(v);
    }
    subgraph.addVertex(v);
    visited.add(v.getVertexID());
    for (Edge<V, E> e : v.outEdges()) {
      subgraph.addEdge(e);
      IVertex<V, E, LongWritable, LongWritable> sink = e.getSink();
      if (!visited.contains(sink.getVertexID())) {
        dfs(sink, visited, subgraph);
      }
    }
  }

  
  
  int getPartitionID(IVertex<V, E, LongWritable, LongWritable> v){
    return (int) v.getVertexID() % peer.getNumPeers();
  }
  
  int getPartitionID(long vertexID){
    return (int) vertexID % peer.getNumPeers();
  }
  
  public EdgeListReader(BSPPeer<LongWritable, Text, KOut, VOut, GraphJobMessage<S, V, E, M>> peer, Partition<S, V, E, M> partition) {
    this.peer = peer;
    this.partition = partition;
  }
  
  /*TODO: Move this to GraphJobRunner. */
  public static <S extends Writable, V extends Writable, E extends Writable, M extends Writable> Subgraph<S, V, E, M> newSubgraphInstance(Class<?> subgraphClass) {
    return (Subgraph<S, V, E, M>) ReflectionUtils.newInstance(subgraphClass);
  }

}
