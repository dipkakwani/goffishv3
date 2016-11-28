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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
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

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.humus.api.IControlMessage;
import in.dream_lab.goffish.humus.api.IReader;
import in.dream_lab.goffish.api.IMessage;


/* Reads graph in the adjacency list format:
 * VID PartitionID Sink1 Sink2 ...
 */
public class LongTextAdjacencyListReader<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable> 
 implements IReader <Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {
  
  Map<LongWritable, IVertex<V, E, LongWritable, LongWritable>> vertexMap;
  BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private Map<K, Integer> subgraphPartitionMap;
  private Map<LongWritable, LongWritable> vertexSubgraphMap;
  
  public LongTextAdjacencyListReader(BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer,Map<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
    this.vertexSubgraphMap = new HashMap<LongWritable, LongWritable>();
  }
  
  
  /*
   * Returns the list of subgraphs belonging to the current partition 
   */
  @Override
  public  List<ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {
    Map<IntWritable, List<Vertex<V, E, LongWritable, LongWritable>>> partitionMap = new HashMap<IntWritable, List<Vertex<V, E, LongWritable, LongWritable>>>();
    vertexMap = new HashMap<LongWritable, IVertex<V, E, LongWritable, LongWritable>>();
    long edgeCount = 0;
    
    KeyValuePair<Writable, Writable> pair;
    while ((pair = peer.readNext()) != null) {
      //NOTE: Confirm that data starts from value and not from key.
      String value[] = pair.getValue().toString().split("\\s+");
      LongWritable sourceID = new LongWritable(Long.parseLong(value[0]));
      IntWritable partitionID = new IntWritable(Integer.parseInt(value[1]));
      //System.out.println("Partition ID: " + partitionID + " source ID: " + sourceID);
      List<Vertex<V, E, LongWritable, LongWritable>> partitionVertices = partitionMap.get(partitionID);
      if (partitionVertices == null) {
        partitionVertices = new ArrayList<Vertex<V, E, LongWritable, LongWritable>>();
        partitionMap.put(partitionID, partitionVertices);
      }
      Vertex<V, E, LongWritable, LongWritable> source = (Vertex<V, E, LongWritable, LongWritable>)vertexMap.get(sourceID);
      if (source == null) {
        source = new Vertex<V, E, LongWritable, LongWritable>(sourceID);
        vertexMap.put(sourceID, source);
      }
      partitionVertices.add(source);
      for (int i = 2; i < value.length; i++) {
        LongWritable sinkID = new LongWritable(Long.parseLong(value[i]));
        //System.out.print(sinkID + "  ");
        Vertex<V, E, LongWritable, LongWritable> sink = (Vertex<V, E, LongWritable, LongWritable>)vertexMap.get(sinkID);   
        if (sink == null) {
          sink = new Vertex<V, E, LongWritable, LongWritable>(sinkID);
          vertexMap.put(sinkID, sink);
        }
        LongWritable edgeID = new LongWritable(edgeCount++ | (((long) peer.getPeerIndex()) << 32));
        Edge<E, LongWritable, LongWritable> e = new Edge<E, LongWritable, LongWritable>(edgeID, sinkID);
        source.addEdge(e);
      }
    }
   
    vertexMap = new HashMap<LongWritable, IVertex<V, E, LongWritable, LongWritable>>();

    //System.out.println("Partition 0 " + partitionMap.containsKey(new IntWritable(0)));

    //System.out.println("Partition 1 " + partitionMap.containsKey(1));
    List<IEdge<E, LongWritable, LongWritable>> _edges = new ArrayList<IEdge<E, LongWritable, LongWritable>>();
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
        Message<LongWritable, LongWritable> msg = new Message<LongWritable, LongWritable>();
        ControlMessage controlInfo = new ControlMessage();
        controlInfo.setTransmissionType(IControlMessage.TransmissionType.VERTEX);
        controlInfo.setVertexValues(sb.toString());
        msg.setControlInfo(controlInfo);
        peer.send(peer.getPeerName(entry.getKey().get()), (Message<K, M>)msg);
      } else { // Belongs to this partition
        //System.out.println("Local " + peer.getPeerIndex() + " = " + entry.getKey() + " size " + entry.getValue().size());
        for (Vertex<V, E, LongWritable, LongWritable> v : entry.getValue()) {
          vertexMap.put(v.getVertexID(), v);
          _edges.addAll(v.outEdges());
        }
      }
    }
    
    //System.out.println("=size="+vertexMap.size());

    // End of first superstep.
    peer.sync();
    Message<LongWritable, LongWritable> msg;
    while ((msg = (Message<LongWritable, LongWritable>)peer.getCurrentMessage()) != null) {
      //System.out.println("Recieved some vertices");
      String msgString = msg.getControlInfo().toString();
      String msgStringArr[] = msgString.split(",");
      for (int i = 0; i < msgStringArr.length; i++) {
        String vertexInfo[] = msgStringArr[i].split(" ");
        LongWritable vertexID = new LongWritable(Long.parseLong(vertexInfo[0]));
        Vertex<V, E, LongWritable, LongWritable> source = (Vertex<V, E, LongWritable, LongWritable>)vertexMap.get(vertexID);
        if (source == null) {
          source = new Vertex<V, E, LongWritable, LongWritable>(vertexID);
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

    //System.out.println("After receiving vertices size " +vertexMap.size());
    /* Create remote vertex objects. */
    for (IEdge<E, LongWritable, LongWritable> e : _edges) {
      LongWritable sinkID = e.getSinkVertexID();
      IVertex<V, E, LongWritable, LongWritable> sink =  vertexMap.get(sinkID);
      if (sink == null) {
        sink = new RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>(sinkID);
        vertexMap.put(sinkID, sink);
      }
    }
    //System.out.println("Total Number of vertices after adding remote Vertices = "+vertexMap.size());

    Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition = new Partition<S, V, E, LongWritable, LongWritable, LongWritable>(peer.getPeerIndex());
    
    formSubgraphs(partition, vertexMap.values());
    
    System.out.flush();
    
    //System.out.println("Number of subgraphs in partition " + peer.getPeerIndex() + " is: " + partition.getSubgraphs().size());
    
    /*
     * Ask Remote vertices to send their subgraph IDs. Requires 2 supersteps
     * because the graph is directed
     */
    Message<LongWritable, LongWritable> question = new Message<LongWritable, LongWritable>();
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    /*
     * Message format being sent:
     * partitionID remotevertex1 remotevertex2 ...
     */
    byte partitionIDbytes[] = Ints.toByteArray(peer.getPeerIndex());
    System.out.println("PArtition ID "+peer.getPeerIndex()+" "+partitionIDbytes);
    controlInfo.addextraInfo(partitionIDbytes);
    question.setControlInfo(controlInfo);
    for (IVertex<V, E, LongWritable, LongWritable> v : vertexMap.values()) {
      if (v instanceof RemoteVertex) {
        byte vertexIDbytes[] = Longs.toByteArray(v.getVertexID().get());
        System.out.println("REmote Vertex "+v.getVertexID().get()+" "+vertexIDbytes);
        controlInfo.addextraInfo(vertexIDbytes);
      }
    }
    sendToAllPartitions(question);

    peer.sync();
    System.out.println("Next SuperStep");

    Map<Integer, List<Message<LongWritable, LongWritable>>> replyMessages = new HashMap<Integer, List<Message<LongWritable, LongWritable>>>();
    //Receiving 1 message per partition
    while ((msg = (Message<LongWritable, LongWritable>) peer.getCurrentMessage()) != null) {
      /*
       * Subgraph Partition mapping broadcast
       * Format of received message:
       * partitionID subgraphID1 subgraphID2 ...
       */
      if (msg.getMessageType() == Message.MessageType.SUBGRAPH) {
        Iterable<BytesWritable> subgraphList = ((ControlMessage) msg
            .getControlInfo()).getExtraInfo();
        
        Integer partitionID = Ints.fromByteArray(subgraphList.iterator().next().getBytes());
        
        for (BytesWritable subgraphListElement : Iterables.skip(subgraphList,1)) {
          LongWritable subgraphID = new LongWritable(
              Longs.fromByteArray(subgraphListElement.getBytes()));
          subgraphPartitionMap.put((K) subgraphID, partitionID);
        }
        // System.out.println(msgString+"Subgraph Broadcast recieved");
        continue;
      }
      
      /*
       * receiving query to find subgraph id Remote Vertex
       */
      Iterable<BytesWritable> RemoteVertexQuery = ((ControlMessage) msg
          .getControlInfo()).getExtraInfo();
      
      /*
       * Reply format :
       * sinkID1 subgraphID1 sinkID2 subgraphID2 ...
       */
      Message<LongWritable, LongWritable> subgraphIDReply = new Message<LongWritable, LongWritable>(); 
      controlInfo = new ControlMessage();
      controlInfo.setTransmissionType(IControlMessage.TransmissionType.NORMAL);
      subgraphIDReply.setControlInfo(controlInfo);
      
      Integer sinkPartition = Ints.fromByteArray(RemoteVertexQuery.iterator().next().getBytes());
      boolean hasAVertex = false;
      for (BytesWritable remoteVertex : Iterables.skip(RemoteVertexQuery,1)) {
        LongWritable sinkID = new LongWritable(Ints.fromByteArray(remoteVertex.getBytes()));
        LongWritable sinkSubgraphID = vertexSubgraphMap.get(sinkID);
        //In case this partition does not have the vertex
        if (sinkSubgraphID == null) {
          continue;
        }
        hasAVertex = true;
        byte sinkIDbytes[] = Longs.toByteArray(sinkID.get());
        controlInfo.addextraInfo(sinkIDbytes);
        byte subgraphIDbytes[] = Longs.toByteArray(sinkSubgraphID.get());
        controlInfo.addextraInfo(subgraphIDbytes);
      }
      if (hasAVertex) {
        peer.send(peer.getPeerName(sinkPartition.intValue()),
            (Message<K, M>) subgraphIDReply);
      }
    }
    peer.sync();
    // System.out.println("Messages to all neighbours sent");

    while ((msg = (Message<LongWritable, LongWritable>)peer.getCurrentMessage()) != null) {
      Iterable<BytesWritable> remoteVertexReply = ((ControlMessage) msg
          .getControlInfo()).getExtraInfo();
      
      Iterator<BytesWritable> queryResponse = remoteVertexReply.iterator();
      while(queryResponse.hasNext()) {
        LongWritable sinkID = new LongWritable(Longs.fromByteArray(queryResponse.next().getBytes()));
        LongWritable remoteSubgraphID = new LongWritable(Longs.fromByteArray(queryResponse.next().getBytes()));
        RemoteVertex<V, E, LongWritable, LongWritable, LongWritable> sink =(RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>) vertexMap.get(sinkID);
        assert(sink!=null);
        if (sink == null) {
          System.out.println("NULLLL");
        }
        sink.setSubgraphID(remoteSubgraphID);
      }
    }

    return partition.getSubgraphs();
  }
  
  /* takes partition and message list as argument and sends the messages to their respective partition.
   * Needed to send messages just before peer.sync(),as a hama bug causes the program to stall while trying
   * to send and recieve(iterate over recieved message) large messages at the same time
   */
  private void sendMessage(int partition,
      List<Message<LongWritable, LongWritable>> messageList) throws IOException {
    
    for (Message<LongWritable, LongWritable> message : messageList) {
      peer.send(peer.getPeerName(partition), (Message<K, M>)message);
    }
    
  }
  
  private void sendToAllPartitions(Message<LongWritable, LongWritable> message) throws IOException {
    for (String peerName : peer.getAllPeerNames()) {
      peer.send(peerName, (Message<K, M>) message);
    }
  }


  /* Forms subgraphs by finding (weakly) connected components. */
  void formSubgraphs(Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition, Collection<IVertex<V, E, LongWritable, LongWritable>> vertices) throws IOException {
    long subgraphCount = 0;
    Set<LongWritable> visited = new HashSet<LongWritable>();  
    //System.out.println(" Size " + vertices.size()+ "=size=" + vertexMap.size());
    
    /*
    for (IVertex<V, E, LongWritable, LongWritable> v : vertices) {
      if (!visited.contains(v.getVertexID()) && !v.isRemote()) {
        LongWritable subgraphID = new LongWritable(subgraphCount++ | (((long) partition.getPartitionID()) << 32));
        Subgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph = new Subgraph<S, V, E, LongWritable, LongWritable, LongWritable>(peer.getPeerIndex(), subgraphID);
        //BFS
        Queue<LongWritable> Q = new LinkedList<LongWritable>();
        Q.add(v.getVertexID());
        System.out.println("Starting vertex for BFS " + v.getVertexID());
        while (!Q.isEmpty()) {
          LongWritable vertexID = Q.poll();
          if(visited.contains(vertexID)) {
            continue;
          }
          visited.add(vertexID);
          IVertex<V, E, LongWritable, LongWritable> source = vertexMap.get(vertexID);
          //System.out.println(vertexID+ " "+source.isRemote());
          subgraph.addVertex(source);
          if (source.isRemote()) {
            //remote vertex
            continue;
          }
          for (IEdge<E, LongWritable, LongWritable> e : source.outEdges()) {
            LongWritable sinkID = e.getSinkVertexID();
            if (!visited.contains(sinkID)) {
              Q.add(sinkID);
            }
          }
        }
        partition.addSubgraph(subgraph);
        String msg = peer.getPeerIndex() + "," + subgraphID.toString();
        Message<LongWritable, LongWritable> subgraphLocationBroadcast = new Message<LongWritable, LongWritable>();
        subgraphLocationBroadcast.setMessageType(IMessage.MessageType.SUBGRAPH);
        ControlMessage controlInfo = new ControlMessage();
        controlInfo
            .setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
        controlInfo.setextraInfo(msg);
        subgraphLocationBroadcast.setControlInfo(controlInfo);
        for (String peerName : peer.getAllPeerNames()) {
          peer.send(peerName, (Message<K, M>) subgraphLocationBroadcast);
        }

        System.out.println("Subgraph " + subgraph.getSubgraphID() + "has "
            + subgraph.vertexCount() + " Vertices");
      }
    }
    */
    // initialize disjoint set
    DisjointSets<IVertex<V, E, LongWritable, LongWritable>> ds = new DisjointSets<IVertex<V, E, LongWritable, LongWritable>>(
        vertices.size());
    for (IVertex<V, E, LongWritable, LongWritable> vertex : vertices) {
      ds.addSet(vertex);
    }

    // union edge pairs
    for (IVertex<V, E, LongWritable, LongWritable> vertex : vertices) {
      if (!vertex.isRemote()) {
        for (IEdge<E, LongWritable, LongWritable> edge : vertex.outEdges()) {
          IVertex<V, E, LongWritable, LongWritable> sink = vertexMap
              .get(edge.getSinkVertexID());
          ds.union(vertex, sink);
        }
      }
    }

    Collection<? extends Collection<IVertex<V, E, LongWritable, LongWritable>>> components = ds
        .retrieveSets();

    for (Collection<IVertex<V, E, LongWritable, LongWritable>> component : components) {
      LongWritable subgraphID = new LongWritable(
          subgraphCount++ | (((long) partition.getPartitionID()) << 32));
      Subgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph = new Subgraph<S, V, E, LongWritable, LongWritable, LongWritable>(
          peer.getPeerIndex(), subgraphID);
      
      //Just confirming that a subgraph does not contain only remote vertex
      assert(component.size()==1 ?
          (component.iterator().next() instanceof RemoteVertex ? false : true) :
            true);
/*      
      System.out.print(subgraphID);
      for (IVertex<V, E, LongWritable, LongWritable> vertex : component) {
        subgraph.addVertex(vertex);
        vertexSubgraphMap.put(vertex.getVertexID(), subgraph.getSubgraphID());
        System.out.print(" "+vertex.getVertexID());
      }
      System.out.println();
      */
      partition.addSubgraph(subgraph);
      String msg = peer.getPeerIndex() + "," + subgraphID.toString();
      Message<LongWritable, LongWritable> subgraphLocationBroadcast = new Message<LongWritable, LongWritable>();
      subgraphLocationBroadcast.setMessageType(IMessage.MessageType.SUBGRAPH);
      ControlMessage controlInfo = new ControlMessage();
      controlInfo
          .setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
      controlInfo.setextraInfo(msg.getBytes());
      subgraphLocationBroadcast.setControlInfo(controlInfo);
      for (String peerName : peer.getAllPeerNames()) {
        peer.send(peerName, (Message<K, M>) subgraphLocationBroadcast);
      }

//      System.out.println("Subgraph " + subgraph.getSubgraphID() + "has "
//          + subgraph.vertexCount() + " Vertices");
    }
    System.out.flush();
  }
}
