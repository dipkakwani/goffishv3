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
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.humus.api.IControlMessage;
import in.dream_lab.goffish.humus.api.IReader;
import in.dream_lab.goffish.utils.DisjointSets;
import in.dream_lab.goffish.api.IMessage;

/*
 * Reads the graph from JSON format
 * [srcid,pid, srcvalue, [[sinkid1,edgeid1,edgevalue1], [sinkid2,edgeid2,edgevalue2] ... ]] 
 */

public class LongMapJSONReader<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable>
    implements
    IReader<Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {

  HamaConfiguration conf;
  Map<LongWritable, IVertex<V, E, LongWritable, LongWritable>> vertexMap;
  BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private Map<K, Integer> subgraphPartitionMap;
  private Map<LongWritable, LongWritable> vertexSubgraphMap;

  public LongMapJSONReader(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer,
      Map<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
    this.conf = peer.getConfiguration();
    this.vertexSubgraphMap = new HashMap<LongWritable, LongWritable>();
  }

  public static final Log LOG = LogFactory.getLog(LongMapJSONReader.class);
  
  @Override
  public List<ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {
	  LOG.info("Creating vertices");

    vertexMap = new HashMap<LongWritable, IVertex<V, E, LongWritable, LongWritable>>();

    // List of edges.Used to create RemoteVertices
    List<IEdge<E, LongWritable, LongWritable>> _edges = new ArrayList<IEdge<E, LongWritable, LongWritable>>();

    KeyValuePair<Writable, Writable> pair;
    while ((pair = peer.readNext()) != null) {
      String StringJSONInput = pair.getValue().toString();
        Vertex<V, E, LongWritable, LongWritable> vertex = createVertex(
            StringJSONInput);
        vertexMap.put(vertex.getVertexID(), vertex);
        _edges.addAll(vertex.outEdges());
      }
    
    LOG.info("Sending Vertices to respective partitions");
    
    LOG.info("Received all vertices");
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
    Message<LongWritable, LongWritable> question = new Message<LongWritable, LongWritable>();
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    question.setControlInfo(controlInfo);
    /*
     * Message format being sent:
     * partitionID remotevertex1 remotevertex2 ...
     */
    byte partitionIDbytes[] = Ints.toByteArray(peer.getPeerIndex());
    controlInfo.addextraInfo(partitionIDbytes);
    for (IVertex<V, E, LongWritable, LongWritable> v : vertexMap.values()) {
      if (v instanceof RemoteVertex) {
        byte vertexIDbytes[] = Longs.toByteArray(v.getVertexID().get());
        controlInfo.addextraInfo(vertexIDbytes);
      }
    }
    sendToAllPartitions(question);

    peer.sync();
    
    Message<LongWritable, LongWritable> msg;
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
        LongWritable sinkID = new LongWritable(Longs.fromByteArray(remoteVertex.getBytes()));
        LongWritable sinkSubgraphID = vertexSubgraphMap.get(sinkID);
        //In case this partition does not have the vertex 
        /* Case 1 : If vertex does not exist
         * Case 2 : If vertex exists but is remote, then its subgraphID is null
         */
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
    
    while ((msg = (Message<LongWritable, LongWritable>)peer.getCurrentMessage()) != null) {
      Iterable<BytesWritable> remoteVertexReply = ((ControlMessage) msg
          .getControlInfo()).getExtraInfo();
      
      Iterator<BytesWritable> queryResponse = remoteVertexReply.iterator();
      while(queryResponse.hasNext()) {
        LongWritable sinkID = new LongWritable(Longs.fromByteArray(queryResponse.next().getBytes()));
        LongWritable remoteSubgraphID = new LongWritable(Longs.fromByteArray(queryResponse.next().getBytes()));
        RemoteVertex<V, E, LongWritable, LongWritable, LongWritable> sink =(RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>) vertexMap.get(sinkID);
        if (sink == null) {
          System.out.println("NULL"+sink);
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
  
  @SuppressWarnings("unchecked")
  Vertex<V, E, LongWritable, LongWritable> createVertex(String JSONString) {
    JSONArray JSONInput = (JSONArray) JSONValue.parse(JSONString);

    LongWritable sourceID = new LongWritable(
        Long.valueOf(JSONInput.get(0).toString()));
    assert (vertexMap.get(sourceID) == null);

    Vertex<V, E, LongWritable, LongWritable> vertex = new Vertex<V, E, LongWritable, LongWritable>(
        sourceID);
    //fix this
  //assumed value of jsonMap= "key1:type1:value1$ key2:type2:value2$....."
    //type could be Long or String or Double
    String jsonMap=JSONInput.get(1).toString();
    String[] vprop=jsonMap.split(Pattern.quote("$"));
    //key,value property pairs for a vertex
    MapWritable vertexMap=new MapWritable();
    for(int i=0;i<vprop.length;i++){
    	String[] map=vprop[i].split(Pattern.quote(":"));
    	Text key=new Text(map[0]);
    	//FIXME:assuming String values for now
    	Text value=new Text(map[2]);
    	vertexMap.put(key, value);
    }
    
  V vertexValue = (V) vertexMap;
    
    vertex.setValue(vertexValue);

    JSONArray edgeList = (JSONArray) JSONInput.get(2);
    for (Object edgeInfo : edgeList) {
      Object edgeValues[] = ((JSONArray) edgeInfo).toArray();
      LongWritable sinkID = new LongWritable(
          Long.valueOf(edgeValues[0].toString()));
      LongWritable edgeID = new LongWritable(
          Long.valueOf(edgeValues[1].toString()));
      //fix this
      //same format as vertex
      String[] eprop= edgeValues[2].toString().split(Pattern.quote("$"));
      MapWritable edgeMap=new MapWritable();
      for(int i=0;i<eprop.length;i++){
      	String[] map=eprop[i].split(Pattern.quote(":"));
      	Text key=new Text(map[0]);
      	//FIXME:assuming String values for now
      	Text value=new Text(map[2]);
      	edgeMap.put(key, value);
      }
      
      Edge<E, LongWritable, LongWritable> edge = new Edge<E, LongWritable, LongWritable>(
          edgeID, sinkID);
      E edgeValue = (E) edgeMap;
      edge.setValue(edgeValue);
      vertex.addEdge(edge);
    }
    return vertex;
  }
  
  /* Forms subgraphs by finding (weakly) connected components. */
  void formSubgraphs(Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition, Collection<IVertex<V, E, LongWritable, LongWritable>> vertices) throws IOException {
    
    long subgraphCount = 0;
    Message<LongWritable, LongWritable> subgraphLocationBroadcast = new Message<LongWritable, LongWritable>();
    
    subgraphLocationBroadcast.setMessageType(IMessage.MessageType.SUBGRAPH);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo
        .setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    subgraphLocationBroadcast.setControlInfo(controlInfo);
    
    byte partitionBytes[] = Ints.toByteArray(peer.getPeerIndex());
    controlInfo.addextraInfo(partitionBytes);
    
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
      

      for (IVertex<V, E, LongWritable, LongWritable> vertex : component) {
        subgraph.addVertex(vertex);
        
        // Dont add remote vertices to the VertexSubgraphMap as remote vertex subgraphID is unknown
        if (!vertex.isRemote()) {
          vertexSubgraphMap.put(vertex.getVertexID(), subgraph.getSubgraphID());
        }        
      }
      
      partition.addSubgraph(subgraph);
      
      byte subgraphIDbytes[] = Longs.toByteArray(subgraphID.get());
      controlInfo.addextraInfo(subgraphIDbytes); 
     
    }
    sendToAllPartitions(subgraphLocationBroadcast);
  }
}