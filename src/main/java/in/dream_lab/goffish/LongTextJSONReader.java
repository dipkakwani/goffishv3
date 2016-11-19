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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.humus.api.IControlMessage;
import in.dream_lab.goffish.humus.api.IReader;
import in.dream_lab.goffish.api.IMessage;

/*
 * Reads the graph from JSON format
 * [srcid,pid, srcvalue, [[sinkid1,edgeid1,edgevalue1], [sinkid2,edgeid2,edgevalue2] ... ]] 
 */

public class LongTextJSONReader<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable>
    implements
    IReader<Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {

  HamaConfiguration conf;
  Map<LongWritable, IVertex<V, E, LongWritable, LongWritable>> vertexMap;
  BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private Map<K, Integer> subgraphPartitionMap;

  public LongTextJSONReader(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer,
      Map<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
    this.conf = peer.getConfiguration();
  }

  @Override
  public List<ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {

    // Map of partitionID,vertex that do not belong to this partition
    Map<Integer, List<String>> partitionMap = new HashMap<Integer, List<String>>();

    vertexMap = new HashMap<LongWritable, IVertex<V, E, LongWritable, LongWritable>>();

    // List of edges.Used to create RemoteVertices
    List<IEdge<E, LongWritable, LongWritable>> _edges = new ArrayList<IEdge<E, LongWritable, LongWritable>>();

    KeyValuePair<Writable, Writable> pair;
    while ((pair = peer.readNext()) != null) {
      String StringJSONInput = pair.getValue().toString();
      JSONArray JSONInput = (JSONArray) JSONValue.parse(StringJSONInput);

      int partitionID = Integer.parseInt(JSONInput.get(1).toString());

      // Vertex does not belong to this partition
      if (partitionID != peer.getPeerIndex()) {
        List<String> partitionVertices = partitionMap.get(partitionID);
        if (partitionVertices == null) {
          partitionVertices = new ArrayList<String>();
          partitionMap.put(partitionID, partitionVertices);
        }
        partitionVertices.add(StringJSONInput);
      } else {
        Vertex<V, E, LongWritable, LongWritable> vertex = createVertex(
            StringJSONInput);
        vertexMap.put(vertex.getVertexID(), vertex);
        _edges.addAll(vertex.outEdges());
      }
    }

    // Send vertices to their respective partitions
    for (Map.Entry<Integer, List<String>> entry : partitionMap.entrySet()) {
      int partitionID = entry.getKey().intValue();
      List<String> vertices = entry.getValue();
      for (String vertex : vertices) {
        Message<LongWritable, LongWritable> vertexMsg = new Message<LongWritable, LongWritable>();
        ControlMessage controlInfo = new ControlMessage();
        controlInfo
            .setTransmissionType(IControlMessage.TransmissionType.VERTEX);
        controlInfo.setVertexValues(vertex);
        vertexMsg.setControlInfo(controlInfo);
        peer.send(peer.getPeerName(partitionID), (Message<K, M>) vertexMsg);
      }
    }
    
    //End of first SuperStep
    peer.sync();
    Message<LongWritable, LongWritable> msg;
    while ((msg = (Message<LongWritable, LongWritable>)peer.getCurrentMessage()) != null) {
      String JSONVertex = msg.getControlInfo().toString();
      Vertex<V, E, LongWritable, LongWritable> vertex = createVertex(JSONVertex);
      vertexMap.put(vertex.getVertexID(), vertex);
      _edges.addAll(vertex.outEdges());
    }
    
    /* Create remote vertex objects. */
    for (IEdge<E, LongWritable, LongWritable> e : _edges) {
      LongWritable sinkID = e.getSinkVertexID();
      IVertex<V, E, LongWritable, LongWritable> sink =  vertexMap.get(sinkID);
      if (sink == null) {
        sink = new RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>(sinkID);
        vertexMap.put(sinkID, sink);
      }
    }
    
    //Direct Copy paste from here
    Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition = new Partition<S, V, E, LongWritable, LongWritable, LongWritable>(peer.getPeerIndex());
    
    formSubgraphs(partition, vertexMap.values());
    
    /*
     * Ask Remote vertices to send their subgraph IDs. Requires 2 supersteps
     * because the graph is directed
     */
    for (IVertex<V, E, LongWritable, LongWritable> v : vertexMap.values()) {
      if (v instanceof RemoteVertex) {
        String s = v.getVertexID() + "," + peer.getPeerIndex();
        for (String peerName : peer.getAllPeerNames()) {
          Message<LongWritable, LongWritable> question = new Message<LongWritable, LongWritable>();
          ControlMessage controlInfo = new ControlMessage();
          controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
          controlInfo.setextraInfo(s.getBytes());
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
        byte rawMsg[] = ((ControlMessage)msg.getControlInfo()).getExtraInfo();
        String msgString = new String(rawMsg);
        System.out.println(msgString+"Subgraph Broadcast recieved");
        String msgStringArr[] = msgString.split(",");
        subgraphPartitionMap.put((K)new LongWritable(Long.valueOf(msgStringArr[1])), Integer.valueOf(msgStringArr[0]));
        continue;
      }
      /*
       * receiving query to find subgraph id Remote Vertex
       */
      byte rawMsg[] = ((ControlMessage)msg.getControlInfo()).getExtraInfo();
      String msgString = new String(rawMsg);
      String msgStringArr[] = msgString.split(",");
      LongWritable sinkID = new LongWritable(Long.valueOf(msgStringArr[0]));
      for (ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph: partition.getSubgraphs()) {
        IVertex<V, E, LongWritable, LongWritable> v = subgraph.getVertexByID(sinkID);
        if (v !=null && !v.isRemote()) {
          String reply = sinkID + "," + subgraph.getSubgraphID();
          Message<LongWritable, LongWritable> subgraphIDReply = new Message<LongWritable, LongWritable>(); 
          ControlMessage controlInfo = new ControlMessage();
          controlInfo.setTransmissionType(IControlMessage.TransmissionType.NORMAL);
          controlInfo.setextraInfo(reply.getBytes());
          subgraphIDReply.setControlInfo(controlInfo);
          peer.send(peer.getPeerName(Integer.parseInt(msgStringArr[1])),(Message<K, M>)subgraphIDReply);
        }
      }
    }
    
    peer.sync();
    System.out.println("Messages to all neighbours sent");
    
    while ((msg = (Message<LongWritable, LongWritable>)peer.getCurrentMessage()) != null) {
      byte rawMsg[] = ((ControlMessage)msg.getControlInfo()).getExtraInfo();
      String msgString = new String(rawMsg);
      //System.out.println("Reply recieved = "+msgString);
      String msgStringArr[] = msgString.split(",");
      LongWritable sinkID = new LongWritable(Long.parseLong(msgStringArr[0]));
      LongWritable remoteSubgraphID = new LongWritable(
          Long.valueOf(msgStringArr[1]));
      for (IVertex<V, E, LongWritable, LongWritable> v : vertexMap.values()) {
        if (v.getVertexID().get() == sinkID.get()) {
          ((RemoteVertex) v).setSubgraphID(remoteSubgraphID);
        }
      }
    }

    return partition.getSubgraphs();

  }
  
  @SuppressWarnings("unchecked")
  Vertex<V, E, LongWritable, LongWritable> createVertex(String JSONString) {
    JSONArray JSONInput = (JSONArray) JSONValue.parse(JSONString);

    LongWritable sourceID = new LongWritable(
        Long.valueOf(JSONInput.get(0).toString()));
    assert (vertexMap.get(sourceID) == null);

    Vertex<V, E, LongWritable, LongWritable> vertex = new Vertex<V, E, LongWritable, LongWritable>(
        sourceID);
    //fix this
    V value = (V) new Text(JSONInput.get(2).toString());
    
    vertex.setValue(value);

    JSONArray edgeList = (JSONArray) JSONInput.get(3);
    for (Object edgeInfo : edgeList) {
      Object edgeValues[] = ((JSONArray) edgeInfo).toArray();
      LongWritable sinkID = new LongWritable(
          Long.valueOf(edgeValues[0].toString()));
      LongWritable edgeID = new LongWritable(
          Long.valueOf(edgeValues[1].toString()));
      //fix this
      E edgeValue = (E) new Text(edgeValues[2].toString());
      
      Edge<E, LongWritable, LongWritable> edge = new Edge<E, LongWritable, LongWritable>(
          edgeID, sinkID);
      edge.setValue(edgeValue);
      vertex.addEdge(edge);
    }
    return vertex;
  }
  
  /* Forms subgraphs by finding (weakly) connected components. */
  void formSubgraphs(Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition, Collection<IVertex<V, E, LongWritable, LongWritable>> vertices) throws IOException {
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
        controlInfo.setextraInfo(msg.getBytes());
        subgraphLocationBroadcast.setControlInfo(controlInfo);
        for (String peerName : peer.getAllPeerNames()) {
          peer.send(peerName, (Message<K, M>) subgraphLocationBroadcast);
        }

        System.out.println("Subgraph " + subgraph.getSubgraphID() + "has "
            + subgraph.vertexCount() + " Vertices");
      }
    }
  }

}
