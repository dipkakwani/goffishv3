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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;


/* Reads graph in the adjacency list format:
 * VID PartitionID StartEdgeID Sink1 Sink2 ...
 */
public class LongTextAdjacencyListReader<S extends Writable, V extends Writable, E extends Writable> 
 implements IReader <Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {
  
  Map<LongWritable, Vertex<V, E, LongWritable, LongWritable>> vertexMap;
  BSPPeer<Writable, Writable, Writable, Writable, IMessage<LongWritable, LongWritable>> peer;
  Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition;

  List<ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {
    Map<IntWritable, List<Vertex<V, E, LongWritable, LongWritable>>> partitionMap = new HashMap<IntWritable, List<Vertex<V, E, LongWritable, LongWritable>>>();
    Map<LongWritable, IVertex<V, E, LongWritable, LongWritable>> vertexMap = new HashMap<LongWritable, IVertex<V, E, LongWritable, LongWritable>>();
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
      Vertex<V, E, LongWritable, LongWritable> source = (Vertex<V, E, LongWritable, LongWritable>)vertexMap.get(sourceID);
      if (source == null) {
        source = new Vertex<V, E, LongWritable, LongWritable>(sourceID);
        vertexMap.put(sourceID, source);
        verticesList.add(source);
      }
      partitionVertices.add(source);
      for (int i = 3; i < value.length; i++) {
        LongWritable sinkID = new LongWritable(Long.parseLong(value[i]));
        Vertex<V, E, LongWritable, LongWritable> sink = (Vertex<V, E, LongWritable, LongWritable>)vertexMap.get(sinkID);   
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
   
    List<IVertex<V, E, LongWritable, LongWritable>> _vertices = new ArrayList<IVertex<V, E, LongWritable, LongWritable>>(); // Final list of vertices.
    vertexMap = new HashMap<LongWritable, IVertex<V, E, LongWritable, LongWritable>>();

    // Send vertices to their respective partitions.
    for (Map.Entry<IntWritable, List<Vertex<V, E, LongWritable, LongWritable>>> entry : partitionMap.entrySet()) {  
      if (entry.getKey().get() != peer.getPeerIndex()) {
        StringBuilder sb = new StringBuilder();
        for (Vertex<V, E, LongWritable, LongWritable> v : entry.getValue()) {
          sb.append(v.getVertexID());
          for (IEdge<E, LongWritable, LongWritable> e : v.outEdges()) {
            Edge<E, LongWritable, LongWritable> edge = (Edge<E, LongWritable, LongWritable>)e;
            sb.append(' ').append(edge.getSinkVertexID()).append(' ').append(edge.getEdgeID());
          }
          sb.append(',');
        }
        Message<LongWritable, LongWritable> msg = new Message<LongWritable, LongWritable>(IMessage.MessageType.VERTEX, entry.getKey().get(), String.valueOf(sb).getBytes());
        peer.send(peer.getPeerName(entry.getKey().get()), msg);
      } else { // Belongs to this partition
        _vertices.addAll(entry.getValue());
        for (Vertex<V, E, LongWritable, LongWritable> v : entry.getValue()) {
          vertexMap.put(v.getVertexID(), v);
        }
      }
    }
    for (IVertex<V, E, LongWritable, LongWritable> v : _vertices) {
      System.out.println(v.getVertexID());
    }
    
    System.out.println(_vertices.size()+"=size="+vertexMap.size());

    // End of first superstep.
    peer.sync();
    IMessage<LongWritable, LongWritable> msg;
    List<Edge<E, LongWritable, LongWritable>> _edges = new ArrayList<Edge<E, LongWritable, LongWritable>>();
    while ((msg = peer.getCurrentMessage()) != null) {
      String msgString = msg.toString();
      String msgStringArr[] = msgString.split(",");
      for (int i = 0; i < msgStringArr.length; i++) {
        String vertexInfo[] = msgStringArr[i].split(" ");
        LongWritable vertexID = new LongWritable(Long.parseLong(vertexInfo[0]));
        Vertex<V, E, LongWritable, LongWritable> source = (Vertex<V, E, LongWritable, LongWritable>)vertexMap.get(vertexID);
        if (source == null) {
          source = new Vertex<V, E, LongWritable, LongWritable>(vertexID);
          _vertices.add(source);
          vertexMap.put(source.getVertexID(), source);
        }
        for (int j = 1; j < vertexInfo.length; j+=2) {
          LongWritable sinkID = new LongWritable(Long.parseLong(vertexInfo[j]));
          LongWritable edgeID = new LongWritable(Long.parseLong(vertexInfo[j + 1]));
          Edge<E, LongWritable, LongWritable> e = new Edge<E, LongWritable, LongWritable>(edgeID, sinkID);
          source.addEdge(e);
          _edges.add(e);
        } 
      }  
    }
    /* Create remote vertex objects. */
    for (Edge<E, LongWritable, LongWritable> e : _edges) {
      LongWritable sinkID = e.getSinkVertexID();
      IVertex<V, E, LongWritable, LongWritable> sink =  vertexMap.get(sinkID);
      if (sink == null) {
        sink = new RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>(sinkID);
        _vertices.add(sink);
      }
    }    

    formSubgraphs(partition, _vertices);
    
    /*
     * Ask Remote vertices to send their subgraph IDs. Requires 2 supersteps
     * because the graph is directed
     */
    for (IVertex<V, E, LongWritable, LongWritable> v : _vertices) {
      if (v instanceof RemoteVertex) {
        String s = v.getVertexID() + "," + peer.getPeerIndex();
        for (String peerName : peer.getAllPeerNames()) {
          Message<LongWritable, LongWritable> question = new Message<LongWritable, LongWritable>(IMessage.MessageType.QUERY, s.getBytes());
          peer.send(peerName, question);
        }
      }
    }

    peer.sync();

    while ((msg = peer.getCurrentMessage()) != null) {
      String msgString = msg.toString();
      String msgStringArr[] = msgString.split(",");
      LongWritable sinkID = new LongWritable(Long.valueOf(msgStringArr[0]));
      for (ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph: partition.getSubgraphs()) {
        IVertex v = subgraph.getVertexByID(sinkID);
        if (v !=null) {
          String reply = sinkID + "," + subgraph.getSubgraphID();
          Message<LongWritable, LongWritable> subgraphIDReply = new Message<LongWritable, LongWritable>(IMessage.MessageType.CUSTOM_MESSAGE,reply.getBytes()); 
          peer.send(peer.getPeerName(Integer.parseInt(msgStringArr[1])),subgraphIDReply);
        }
      }
    }
    
    peer.sync();
    System.out.println("Messages to all neighbours sent");
    
    while ((msg = peer.getCurrentMessage()) != null) {
      String msgString = msg.toString();
      String msgStringArr[] = msgString.split(",");
      LongWritable sinkID = new LongWritable(Long.parseLong(msgStringArr[0]));
      LongWritable remoteSubgraphID = new LongWritable(
          Long.valueOf(msgStringArr[1]));
      for (IVertex<V, E, LongWritable, LongWritable> v : _vertices) {
        if (v.getVertexID() == sinkID) {
          ((RemoteVertex) v).setSubgraphID(remoteSubgraphID);
        }
      }
    }

    return partition.getSubgraphs();
  }
  
  /* Forms subgraphs by finding (weakly) connected components. */
  void formSubgraphs(Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition, List<IVertex<V, E, LongWritable, LongWritable>> vertices) {
    long subgraphCount = 0;
    Set<Long> visited = new HashSet<Long>();

    for (IVertex<V, E, LongWritable, LongWritable> v : vertices) {
      if (!visited.contains(v.getVertexID())) {
        LongWritable subgraphID = new LongWritable(subgraphCount++ | (((long) partition.getPartitionID()) << 32));
        Subgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph = new Subgraph<S, V, E, LongWritable, LongWritable, LongWritable>(peer.getPeerIndex(), subgraphID);
        //BFS
        Queue<LongWritable> Q = new LinkedList<LongWritable>();
        Q.add(v.getVertexID());
        subgraph.addVertex(v);
        while (!Q.isEmpty()) {
          LongWritable vertexID = Q.poll();
          IVertex<V, E, LongWritable, LongWritable> source = vertexMap.get(vertexID);
          for (IEdge<E, LongWritable, LongWritable> e : source.outEdges()) {
            IVertex<V, E, LongWritable, LongWritable> sink = (IVertex<V, E, LongWritable, LongWritable>) e.getSinkVertexID();
            if (!visited.contains(sink.getVertexID())) {
              subgraph.addVertex(sink);
              Q.add(sink.getVertexID());
            }
          }
        }
        partition.addSubgraph(subgraph);
        System.out.println("Subgraph " + subgraph.getSubgraphID() + "has "
            + subgraph.vertexCount() + "Vertices");
      }
    }
  }
  
  public LongTextAdjacencyListReader(BSPPeer<Writable, Writable, Writable, Writable, IMessage<LongWritable, LongWritable>> peer, Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition) {
    this.peer = peer;
    this.partition = partition;
  }

}
