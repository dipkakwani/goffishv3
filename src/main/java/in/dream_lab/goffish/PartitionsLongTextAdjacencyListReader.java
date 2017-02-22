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
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.humus.api.IControlMessage;
import in.dream_lab.goffish.humus.api.IReader;
import in.dream_lab.goffish.utils.DisjointSets;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;

/* Reads graph in the adjacency list format:
 * VID Sink1 Sink2 ...
 */
public class PartitionsLongTextAdjacencyListReader<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable>
    implements
    IReader<Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {

  public static final Log LOG = LogFactory
      .getLog(PartitionsLongTextAdjacencyListReader.class);

  Map<Long, IVertex<V, E, LongWritable, LongWritable>> vertexMap;
  Map<Long, IRemoteVertex<V, E, LongWritable, LongWritable, LongWritable>> remoteVertexMap;
  BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private Map<K, Integer> subgraphPartitionMap;
  // TODO : Change to Long from LongWritable
  private Map<LongWritable, LongWritable> vertexSubgraphMap;

  public PartitionsLongTextAdjacencyListReader(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer,
      Map<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
    this.vertexSubgraphMap = new HashMap<LongWritable, LongWritable>();
  }

  /*
   * Returns the list of subgraphs belonging to the current partition
   */
  @Override
  public List<ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {

    /* Used for logging */
    Runtime runtime = Runtime.getRuntime();
    int mb = 1024*1024;
    

    LOG.info("Free Memory in Reader: " + runtime.freeMemory() / mb + " Total Memory: " + runtime.totalMemory() / mb);
    
    LOG.info("Free Memory after Reaching reader " + Runtime.getRuntime().freeMemory());
    System.out.println(Runtime.getRuntime().freeMemory());
    
    KeyValuePair<Writable, Writable> pair;
    pair = peer.readNext();
    String metaInfo[] = pair.getValue().toString().trim().split(" ");
    long localVertexCount = Long.parseLong(metaInfo[0]);
    long partitionEdgeCount = Long.parseLong(metaInfo[1]);
    long totalPartitionVertices = Long.parseLong(metaInfo[2]);
    long edgeCount = 0;

    vertexMap = Maps.newHashMapWithExpectedSize((int)localVertexCount);
    remoteVertexMap = Maps.newHashMapWithExpectedSize((int)(totalPartitionVertices - localVertexCount));
    
    LOG.info("SETUP Starting Free Memory: " + runtime.freeMemory() / mb + " Total Memory: " + runtime.totalMemory() / mb);
    LOG.info("SETUP Starting " + peer.getPeerIndex() + " Memory: "
        + Runtime.getRuntime().freeMemory());

    while ((pair = peer.readNext()) != null) {
      // NOTE: Confirm that data starts from value and not from key.
      String stringInput = pair.getValue().toString();
      String vertexValue[] = stringInput.split("\\s+");
      
      LongWritable vertexID = new LongWritable(Long.parseLong(vertexValue[0]));
      Vertex<V, E, LongWritable, LongWritable> vertex = new Vertex<V, E, LongWritable, LongWritable>();
      vertex.setVertexID(vertexID);

      for (int j = 1; j < vertexValue.length; j++) {
        LongWritable sinkID = new LongWritable(Long.parseLong(vertexValue[j]));
        LongWritable edgeID = new LongWritable(
                edgeCount++ | (((long) peer.getPeerIndex()) << 32));
        Edge<E, LongWritable, LongWritable> e = new Edge<E, LongWritable, LongWritable>(edgeID, sinkID);
        vertex.addEdge(e);
      }

      vertexMap.put(vertex.getVertexID().get(), vertex);

    }

    LOG.info("Number of Vertices: " + vertexMap.size() + " Edges: " + edgeCount);

    LOG.info("Free Memory: " + runtime.freeMemory() / mb + " Total Memory: " + runtime.totalMemory() / mb);
    //System.gc();
    LOG.info("Creating Remote Vertex Objects");


    //creating a copy to prevent concurrentaccess exception
/*    List<IVertex<V, E, LongWritable, LongWritable>> vertices = Lists.newLinkedList();//new ArrayList<>(vertexMap.size());
    for (IVertex<V, E, LongWritable, LongWritable> v : vertexMap.values()) {
      vertices.add(v);
    }
*/
    /* Create remote vertex objects. */
    for (IVertex<V, E, LongWritable, LongWritable> vertex : vertexMap.values()) {
      for (IEdge<E, LongWritable, LongWritable> e : vertex.outEdges()) {
        LongWritable sinkID = e.getSinkVertexID();
        IVertex<V, E, LongWritable, LongWritable> sink = vertexMap.get(sinkID.get());
        if (sink == null) {
          sink = new RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>(
              sinkID);
          remoteVertexMap.put(sinkID.get(), (IRemoteVertex<V, E, LongWritable, LongWritable, LongWritable>)sink);
        }
      }
    }
    //_edges = null;
    
    Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition = new Partition<S, V, E, LongWritable, LongWritable, LongWritable>(
        peer.getPeerIndex());

    LOG.info("Calling formSubgraph()");

    formSubgraphs(partition);

    LOG.info("Done with formSubgraph()");
    /*
     * Ask Remote vertices to send their subgraph IDs. Requires 2 supersteps
     * because the graph is directed
     */
    Message<LongWritable, LongWritable> question = new Message<LongWritable, LongWritable>();
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    question.setControlInfo(controlInfo);
    /*
     * Message format being sent: partitionID remotevertex1 remotevertex2 ...
     */
    byte partitionIDbytes[] = Ints.toByteArray(peer.getPeerIndex());
    controlInfo.addextraInfo(partitionIDbytes);
    for (IVertex<V, E, LongWritable, LongWritable> v : remoteVertexMap.values()) {
      byte vertexIDbytes[] = Longs.toByteArray(v.getVertexID().get());
      controlInfo.addextraInfo(vertexIDbytes);
    }
    sendToAllPartitions(question);

    LOG.info("Completed first superstep in reader");
    peer.sync();
    LOG.info("Started superstep 2 in reader");

    Message<LongWritable, LongWritable> msg;
    Map<Integer, List<Message<LongWritable, LongWritable>>> replyMessages = new HashMap<Integer, List<Message<LongWritable, LongWritable>>>();
    // Receiving 1 message per partition
    while ((msg = (Message<LongWritable, LongWritable>) peer
        .getCurrentMessage()) != null) {
      /*
       * Subgraph Partition mapping broadcast Format of received message:
       * partitionID subgraphID1 subgraphID2 ...
       */
      if (msg.getMessageType() == Message.MessageType.SUBGRAPH) {
        Iterable<BytesWritable> subgraphList = ((ControlMessage) msg
            .getControlInfo()).getExtraInfo();

        Integer partitionID = Ints
            .fromByteArray(subgraphList.iterator().next().getBytes());

        for (BytesWritable subgraphListElement : Iterables.skip(subgraphList,
            1)) {
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
       * Reply format : sinkID1 subgraphID1 sinkID2 subgraphID2 ...
       */
      Message<LongWritable, LongWritable> subgraphIDReply = new Message<LongWritable, LongWritable>();
      controlInfo = new ControlMessage();
      controlInfo.setTransmissionType(IControlMessage.TransmissionType.NORMAL);
      subgraphIDReply.setControlInfo(controlInfo);

      Integer sinkPartition = Ints
          .fromByteArray(RemoteVertexQuery.iterator().next().getBytes());
      boolean hasAVertex = false;
      for (BytesWritable remoteVertex : Iterables.skip(RemoteVertexQuery, 1)) {
        LongWritable sinkID = new LongWritable(
            Longs.fromByteArray(remoteVertex.getBytes()));
        LongWritable sinkSubgraphID = vertexSubgraphMap.get(sinkID);
        // In case this partition does not have the vertex
        /*
         * Case 1 : If vertex does not exist Case 2 : If vertex exists but is
         * remote, then its subgraphID is null
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
    
    LOG.info("Completed 2nd superstep in reader");
    peer.sync();
    LOG.info("Starting 3rd superstep in reader");

    while ((msg = (Message<LongWritable, LongWritable>) peer
        .getCurrentMessage()) != null) {
      Iterable<BytesWritable> remoteVertexReply = ((ControlMessage) msg
          .getControlInfo()).getExtraInfo();

      Iterator<BytesWritable> queryResponse = remoteVertexReply.iterator();
      while (queryResponse.hasNext()) {
        LongWritable sinkID = new LongWritable(
            Longs.fromByteArray(queryResponse.next().getBytes()));
        LongWritable remoteSubgraphID = new LongWritable(
            Longs.fromByteArray(queryResponse.next().getBytes()));
        RemoteVertex<V, E, LongWritable, LongWritable, LongWritable> sink = (RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>) vertexMap
            .get(sinkID.get());
        if (sink == null) {
          System.out.println("NULLLL" + sink);
        }
        sink.setSubgraphID(remoteSubgraphID);
      }
    }
    
    LOG.info("Reader finished");
    return partition.getSubgraphs();
  }

  private void sendToAllPartitions(Message<LongWritable, LongWritable> message)
      throws IOException {
    for (String peerName : peer.getAllPeerNames()) {
      peer.send(peerName, (Message<K, M>) message);
    }
  }

  /* Forms subgraphs by finding (weakly) connected components. */
  void formSubgraphs(
      Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition)
      throws IOException {

    long subgraphCount = 0;
    Message<LongWritable, LongWritable> subgraphLocationBroadcast = new Message<LongWritable, LongWritable>();

    subgraphLocationBroadcast.setMessageType(IMessage.MessageType.SUBGRAPH);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    subgraphLocationBroadcast.setControlInfo(controlInfo);

    byte partitionBytes[] = Ints.toByteArray(peer.getPeerIndex());
    controlInfo.addextraInfo(partitionBytes);

    
    // initialize disjoint set
    DisjointSets<IVertex<V, E, LongWritable, LongWritable>> ds = new DisjointSets<IVertex<V, E, LongWritable, LongWritable>>(
        vertexMap.size() + remoteVertexMap.size());
    for (IVertex<V, E, LongWritable, LongWritable> vertex : vertexMap.values()) {
      ds.addSet(vertex);
    }
    for (IVertex<V, E, LongWritable, LongWritable> vertex : remoteVertexMap.values()) {
      ds.addSet(vertex);
    }

    // union edge pairs
    for (IVertex<V, E, LongWritable, LongWritable> vertex : vertexMap.values()) {
      for (IEdge<E, LongWritable, LongWritable> edge : vertex.outEdges()) {
        IVertex<V, E, LongWritable, LongWritable> sink = vertexMap
            .get(edge.getSinkVertexID().get());
        ds.union(vertex, sink);
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

        // Dont add remote vertices to the VertexSubgraphMap as remote vertex
        // subgraphID is unknown
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
