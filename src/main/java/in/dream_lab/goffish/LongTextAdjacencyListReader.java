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
import org.slf4j.helpers.SubstituteLoggerFactory;

import com.sun.tools.classfile.SourceID_attribute;

import in.dream_lab.goffish.IMessage.MessageType;


/* Reads graph in the adjacency list format:
 * VID PartitionID Sink1 Sink2 ...
 */
public class LongTextAdjacencyListReader<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable> 
 implements IReader <Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {
  
  Map<LongWritable, IVertex<V, E, LongWritable, LongWritable>> vertexMap;
  BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private Map<K, Integer> subgraphPartitionMap;
  
  public LongTextAdjacencyListReader(BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer,Map<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
  }
  
  
  /*
   * Returns the list of subgraphs belonging to the current partition 
   */
  @Override
  public  List<ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {
    Map<IntWritable, List<Vertex<V, E, LongWritable, LongWritable>>> partitionMap = new HashMap<IntWritable, List<Vertex<V, E, LongWritable, LongWritable>>>();
    vertexMap = new HashMap<LongWritable, IVertex<V, E, LongWritable, LongWritable>>();
    List<Vertex<V, E, LongWritable, LongWritable>> verticesList = new ArrayList<Vertex<V, E, LongWritable, LongWritable>>();
    long edgeCount = 0;
    
    KeyValuePair<Writable, Writable> pair;
    long numPeers = peer.getNumPeers();
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
        verticesList.add(source);
      }
      partitionVertices.add(source);
      for (int i = 2; i < value.length; i++) {
        LongWritable sinkID = new LongWritable(Long.parseLong(value[i]));
        //System.out.print(sinkID + "  ");
        Vertex<V, E, LongWritable, LongWritable> sink = (Vertex<V, E, LongWritable, LongWritable>)vertexMap.get(sinkID);   
        if (sink == null) {
          sink = new Vertex<V, E, LongWritable, LongWritable>(sinkID);
          vertexMap.put(sinkID, sink);
          verticesList.add(sink);
        }
        LongWritable edgeID = new LongWritable(edgeCount++ | (((long) peer.getPeerIndex()) << 32));
        Edge<E, LongWritable, LongWritable> e = new Edge<E, LongWritable, LongWritable>(edgeID, sinkID);
        source.addEdge(e);
      }
      //System.out.println();
    }
   
    List<IVertex<V, E, LongWritable, LongWritable>> _vertices = new ArrayList<IVertex<V, E, LongWritable, LongWritable>>(); // Final list of vertices.
    vertexMap = new HashMap<LongWritable, IVertex<V, E, LongWritable, LongWritable>>();

    System.out.println("Partition 0 " + partitionMap.containsKey(new IntWritable(0)));

    System.out.println("Partition 1 " + partitionMap.containsKey(1));
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
        System.out.println("Local " + peer.getPeerIndex() + " = " + entry.getKey() + " size " + entry.getValue().size());
        _vertices.addAll(entry.getValue());
        for (Vertex<V, E, LongWritable, LongWritable> v : entry.getValue()) {
          vertexMap.put(v.getVertexID(), v);
          _edges.addAll(v.outEdges());
        }
      }
    }
    
    System.out.println(_vertices.size()+"=size="+vertexMap.size());

    // End of first superstep.
    peer.sync();
    Message<LongWritable, LongWritable> msg;
//    List<Edge<E, LongWritable, LongWritable>> _edges = new ArrayList<Edge<E, LongWritable, LongWritable>>();
    while ((msg = (Message<LongWritable, LongWritable>)peer.getCurrentMessage()) != null) {
      System.out.println("Recieved some vertices");
      String msgString = msg.getControlInfo().toString();
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

    System.out.println("After receiving vertices size " + _vertices.size()+"=size="+vertexMap.size());
    /* Create remote vertex objects. */
    for (IEdge<E, LongWritable, LongWritable> e : _edges) {
      LongWritable sinkID = e.getSinkVertexID();
      IVertex<V, E, LongWritable, LongWritable> sink =  vertexMap.get(sinkID);
      if (sink == null) {
        sink = new RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>(sinkID);
        _vertices.add(sink);
        vertexMap.put(sinkID, sink);
        /*
        if(sinkID.get()%2==0 && peer.getPeerIndex()==1) {
          System.out.println("Anomaly VertexID = "+sinkID.get());
        }
        else if(sinkID.get()%2!=0 && peer.getPeerIndex()==0) {
          System.out.println("Anomaly VertexID = "+sinkID.get());
        }
        */
      }
    }
    System.out.println("Number of vertices after adding remote Vertices = "+vertexMap.size()+" "+_vertices.size());

    Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition = new Partition<S, V, E, LongWritable, LongWritable, LongWritable>(peer.getPeerIndex());
    
    formSubgraphs(partition, _vertices);
    
    /*
     * Ask Remote vertices to send their subgraph IDs. Requires 2 supersteps
     * because the graph is directed
     */
    for (IVertex<V, E, LongWritable, LongWritable> v : _vertices) {
      if (v instanceof RemoteVertex) {
        String s = v.getVertexID() + "," + peer.getPeerIndex();
        for (String peerName : peer.getAllPeerNames()) {
          Message<LongWritable, LongWritable> question = new Message<LongWritable, LongWritable>();
          ControlMessage controlInfo = new ControlMessage();
          controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
          controlInfo.setextraInfo(s);
          question.setControlInfo(controlInfo);
          peer.send(peerName, (Message<K, M>)question);
        }
      }
    }

    peer.sync();

    while ((msg = (Message<LongWritable, LongWritable>)peer.getCurrentMessage()) != null) {
      /*
       * Subgraph Partition mapping broadcast
       */
      if (msg.getMessageType() == Message.MessageType.SUBGRAPH) {
        String msgString = ((ControlMessage)msg.getControlInfo()).getExtraInfo();
        System.out.println(msgString+"Subgraph Broadcast recieved");
        String msgStringArr[] = msgString.split(",");
        subgraphPartitionMap.put((K)new LongWritable(Long.valueOf(msgStringArr[1])), Integer.valueOf(msgStringArr[0]));
        continue;
      }
      /*
       * receiving query to find subgraph id Remote Vertex
       */
      String msgString = ((ControlMessage)msg.getControlInfo()).getExtraInfo();
      String msgStringArr[] = msgString.split(",");
      LongWritable sinkID = new LongWritable(Long.valueOf(msgStringArr[0]));
      for (ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph: partition.getSubgraphs()) {
        IVertex<V, E, LongWritable, LongWritable> v = subgraph.getVertexByID(sinkID);
        if (v !=null && !v.isRemote()) {
          String reply = sinkID + "," + subgraph.getSubgraphID();
          Message<LongWritable, LongWritable> subgraphIDReply = new Message<LongWritable, LongWritable>(); 
          ControlMessage controlInfo = new ControlMessage();
          controlInfo.setTransmissionType(IControlMessage.TransmissionType.NORMAL);
          controlInfo.setextraInfo(reply);
          subgraphIDReply.setControlInfo(controlInfo);
          peer.send(peer.getPeerName(Integer.parseInt(msgStringArr[1])),(Message<K, M>)subgraphIDReply);
        }
      }
    }
    
    peer.sync();
    System.out.println("Messages to all neighbours sent");
    
    while ((msg = (Message<LongWritable, LongWritable>)peer.getCurrentMessage()) != null) {
      String msgString = ((ControlMessage)msg.getControlInfo()).getExtraInfo();
      //System.out.println("Reply recieved = "+msgString);
      String msgStringArr[] = msgString.split(",");
      LongWritable sinkID = new LongWritable(Long.parseLong(msgStringArr[0]));
      LongWritable remoteSubgraphID = new LongWritable(
          Long.valueOf(msgStringArr[1]));
      //TODO: Why not use the map instead?
      for (IVertex<V, E, LongWritable, LongWritable> v : _vertices) {
        if (v.getVertexID().get() == sinkID.get()) {
          ((RemoteVertex) v).setSubgraphID(remoteSubgraphID);
          //System.out.println(remoteSubgraphID==null);
        }
      }
    }

    return partition.getSubgraphs();
  }
  
  /* Forms subgraphs by finding (weakly) connected components. */
  void formSubgraphs(Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition, List<IVertex<V, E, LongWritable, LongWritable>> vertices) throws IOException {
    long subgraphCount = 0;
    Set<LongWritable> visited = new HashSet<LongWritable>();  
    System.out.println(" Size " + vertices.size()+ "=size=" + vertexMap.size());
    
    for (IVertex<V, E, LongWritable, LongWritable> v : vertices) {
      if (!visited.contains(v.getVertexID()) && !v.isRemote()) {
        LongWritable subgraphID = new LongWritable(subgraphCount++ | (((long) partition.getPartitionID()) << 32));
        Subgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph = new Subgraph<S, V, E, LongWritable, LongWritable, LongWritable>(peer.getPeerIndex(), subgraphID);
        //BFS
        Queue<LongWritable> Q = new LinkedList<LongWritable>();
        Q.add(v.getVertexID());
        System.out.println("Starting vertex for BFS " + v.getVertexID());
        //subgraph.addVertex(v);
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
          if (vertexID.get() == 350) {
            System.out.println("Found 350 !");
          }
          for (IEdge<E, LongWritable, LongWritable> e : source.outEdges()) {
            LongWritable sinkID = e.getSinkVertexID();
            if (!visited.contains(sinkID)) {
              //subgraph.addVertex(vertexMap.get(sinkID));
              Q.add(sinkID);
            }
          }
        }
        partition.addSubgraph(subgraph);
        String msg = peer.getPeerIndex()+","+subgraphID.toString();
        Message<LongWritable,LongWritable> subgraphLocationBroadcast = new Message<LongWritable,LongWritable>();
        subgraphLocationBroadcast.setMessageType(IMessage.MessageType.SUBGRAPH);
        ControlMessage controlInfo = new ControlMessage();
        controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
        controlInfo.setextraInfo(msg);
        subgraphLocationBroadcast.setControlInfo(controlInfo);
        for (String peerName : peer.getAllPeerNames()) {
          peer.send(peerName, (Message<K, M>)subgraphLocationBroadcast);
        }
        
        System.out.println("Subgraph " + subgraph.getSubgraphID() + "has "
            + subgraph.vertexCount() + " Vertices");
        /*
        System.out.println("Vertices");
        for (IVertex<V, E, LongWritable, LongWritable> vertex : subgraph.getVertices()) {
          System.out.print(vertex.getVertexID() + " ");
        }
        System.out.println();*/
      }
    }
  }

}
