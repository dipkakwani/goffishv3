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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;

import in.dream_lab.goffish.GraphJob;
import in.dream_lab.goffish.Vertex;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.ISubgraphCompute;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.humus.api.IControlMessage;
import in.dream_lab.goffish.humus.api.IReader;

import org.apache.hama.util.ReflectionUtils;
import org.apache.hama.util.UnsafeByteArrayInputStream;
import org.apache.hama.util.WritableUtils;

import in.dream_lab.goffish.sample.VertexCount;
import in.dream_lab.goffish.sample.ConnectedComponents;;
/**
 * Fully generic graph job runner.
 * 
 * @param <V> the id type of a vertex.
 * @param <E> the value type of an edge.
 * @param <M> the value type of a vertex.
 */
//@SuppressWarnings("rawtypes")
public final class GraphJobRunner<S extends Writable, V extends Writable, E extends Writable, M extends Writable, I extends Writable, J extends Writable, K extends Writable>
    extends BSP<Writable, Writable, Writable, Writable, Message<K, M>> {

  private static final long INITIALIZATION_SUPERSTEPS = 3;
  
  /* Maintains statistics about graph job. Updated by master. */
  public static enum GraphJobCounter {
    ACTIVE_SUBGRAPHS
  }
  
  private Partition<S, V, E, I, J, K> partition;
  private BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private HamaConfiguration conf;
  private Map<K, Integer> subgraphPartitionMap;
  private static Class<?> SUBGRAPH_CLASS;
  //public static Class<Subgraph<?, ?, ?, ?, ?, ?, ?>> subgraphClass;
  private Map<K, List<IMessage<K, M>>> subgraphMessageMap;
  private List<SubgraphCompute<S, V, E, M, I, J, K>> subgraphs=new ArrayList<SubgraphCompute<S, V, E, M, I, J, K>>();
  boolean allVotedToHalt = false, messageInFlight = false, globalVoteToHalt = false;
  
  @Override
  public final void setup(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer)
      throws IOException, SyncException, InterruptedException {

    //System.out.println("BSP Setup");
    setupfields(peer);
    /*TODO: Read input reader class type from Hama conf. 
     * FIXME:Make type of Message generic in Reader. */

    Class<?> readerClass = conf.getClass(Constants.RUNTIME_PARTITION_RECORDCONVERTER, LongTextAdjacencyListReader.class, IReader.class);
    List<Object> params = new ArrayList<Object>();
    params.add(peer);
    params.add(subgraphPartitionMap);
    
    IReader<Writable, Writable, Writable, Writable, S, V, E, I, J, K> reader = 
        //ReflectionUtils.newInstance(readerClass, params);
        (IReader<Writable, Writable, Writable, Writable, S, V, E, I, J, K>)new LongTextAdjacencyListReader<S, V, E, K, M>(peer,subgraphPartitionMap);
    
    int subgraphs = 0;
    for (ISubgraph<S, V, E, I, J, K> subgraph: reader.getSubgraphs()) {
      subgraphs++;
      partition.addSubgraph(subgraph);
      for (IVertex<V, E, I, J> vertex : subgraph.getLocalVertices()) {
        System.out.print(vertex.getVertexID()+" "+peer.getPeerIndex()+" ");
        for (IEdge<E, I, J> edges: vertex.outEdges()) {
          //take care of extra space in the end while comparing
          System.out.print(edges.getSinkVertexID()+" ");
        }
        System.out.println();
      }
    }
    System.out.println(subgraphs);
  }
  
  /*Initialize the  fields*/
  private void setupfields(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer) {
    
    this.peer = peer;
    partition = new Partition<S, V, E, I, J, K>(peer.getPeerIndex());
    this.conf = peer.getConfiguration();
    this.subgraphPartitionMap = new HashMap<K, Integer>();
    this.subgraphMessageMap = new HashMap<K, List<IMessage<K, M>>>();
    /*subgraphClass = (Class<Subgraph<?, ?, ?, ?, ?, ?, ?>>) conf.getClass(
        "hama.subgraph.class", Subgraph.class);
    SUBGRAPH_CLASS = subgraphClass;
    */
    
  }

   
  @Override
  public final void bsp(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer)
      throws IOException, SyncException, InterruptedException {
    
    /*TODO: Make execute subgraphs compute in parallel.
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors
        .newCachedThreadPool();
    executor.setMaximumPoolSize(64);*/
    //System.out.println("BSP method at superstep "+peer.getSuperstepCount());
    
    /*
     * Creating SubgraphCompute objects
     */
    for (ISubgraph<S, V, E, I, J, K> subgraph : partition.getSubgraphs()) {
      
      /* FIXME: Read generic types from configuration and make subgraph object generic. */
      SubgraphCompute subgraphComputeRunner = new VertexCount.VrtxCnt();
      //SubgraphCompute subgraphComputeRunner = new ConnectedComponents.CC();
      subgraphComputeRunner.setSubgraph((ISubgraph<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>)subgraph);
      subgraphComputeRunner.init((GraphJobRunner<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) this);
      subgraphs.add((SubgraphCompute<S, V, E, M, I, J, K>) subgraphComputeRunner);
    }
    
    
    while (!globalVoteToHalt) {     
      List<IMessage<K, M>> messages = new ArrayList<IMessage<K, M>>();
      Message<K, M> msg;
      while ((msg = peer.getCurrentMessage()) != null) {
        messages.add(msg);
      }
      //System.out.println(messages.size()+" Messages");
      subgraphMessageMap = new HashMap<K, List<IMessage<K, M>>>();
      globalVoteToHalt = (isMasterTask(peer) && getSuperStepCount() != 0) ? true : false;
      allVotedToHalt = true;
      messageInFlight = false;
      parseMessage(messages);
      
      if (globalVoteToHalt && isMasterTask(peer)) {
        notifyJobEnd();
        peer.sync();    // Wait for all the peers to receive the message in next superstep.
        break;
      }
      else if (globalVoteToHalt) {
        break;
      }

      for (SubgraphCompute<S, V, E, M, I, J, K> subgraph : subgraphs) {
        //System.out.println("Calling compute with vertices"+subgraph.getSubgraph().localVertexCount());
        boolean hasMessages = false;
        List<IMessage<K, M>> messagesToSubgraph = subgraphMessageMap.get(subgraph.getSubgraph().getSubgraphID());
        if (messagesToSubgraph != null) {
          hasMessages = true;
        }
        if (!subgraph.hasVotedToHalt() || hasMessages) {
          //if (hasMessages)
            //System.out.println("Computing " + subgraph.getSubgraph().getSubgraphID() + " messages " + messagesToSubgraph.size());
          //else
            //System.out.println("Computing " + subgraph.getSubgraph().getSubgraphID() + " 0 messages ");
          subgraph.setActive();
          subgraph.compute(messagesToSubgraph);
          if (!subgraph.hasVotedToHalt())
            allVotedToHalt = false;
        }
      }
      sendHeartBeat();
      
      peer.sync();
    }

  }

  @Override
  public final void cleanup(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer)
      throws IOException {
    //System.out.println("Clean up!");
    for (ISubgraphCompute<S, V, E, M, I, J, K> subgraph : subgraphs) {
      //System.out.println(subgraph.getSubgraph().getValue());
    }
  }

  /*
   * Each peer sends heart beat to the master, which indicates if all the
   * subgraphs has voted to halt and there is no message which is being sent in
   * the current superstep.
   */
  void sendHeartBeat() {
    Message<K, M> msg = new Message<K, M>();
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.HEARTBEAT);
    String allVotedToHaltMsg = (allVotedToHalt) ? "1" : "0";
    String messageInFlightMsg = (messageInFlight) ? "1" : "0";
    String heartBeatMsg = allVotedToHaltMsg + messageInFlightMsg;
    controlInfo.setextraInfo(heartBeatMsg.getBytes());
    msg.setControlInfo(controlInfo);
    sendMessage(peer.getPeerName(getMasterTaskIndex()), msg);
  }
  
  void parseMessage(List<IMessage<K, M>> messages) {
    for (IMessage<K, M> message : messages) {
      //Broadcast message, therefore every subgraph receives it
      if(((Message<K, M>)message).getControlInfo().getTransmissionType() == IControlMessage.TransmissionType.BROADCAST) {
        for (ISubgraph<S, V, E, I, J, K> subgraph : partition.getSubgraphs()) {
          List<IMessage<K, M>> subgraphMessage = subgraphMessageMap.get(subgraph.getSubgraphID());
          if(subgraphMessage == null) {
            subgraphMessage = new ArrayList<IMessage<K, M>>();
            subgraphMessageMap.put(subgraph.getSubgraphID(), subgraphMessage);
          }
          subgraphMessage.add(message);
        }
      }
      else if(((Message<K, M>)message).getControlInfo().getTransmissionType() == IControlMessage.TransmissionType.NORMAL) {
        List<IMessage<K, M>> subgraphMessage = subgraphMessageMap.get(message.getSubgraphID());
        if(subgraphMessage == null) {
          subgraphMessage = new ArrayList<IMessage<K, M>>();
          subgraphMessageMap.put(message.getSubgraphID(), subgraphMessage);
        }
        subgraphMessage.add(message);
      }
      else if(((Message<K, M>)message).getControlInfo().getTransmissionType() == IControlMessage.TransmissionType.GLOBAL_HALT) {
        globalVoteToHalt = true;
      }
      else if(((Message<K, M>)message).getControlInfo().getTransmissionType() == IControlMessage.TransmissionType.HEARTBEAT) {
        assert(isMasterTask(peer));
        parseHeartBeat(message);
      }
      /*
       * TODO: Add implementation for partition message and vertex message(used for graph mutation)
       */
    }
  }
  
  /* Sets global vote to halt to false if any of the peer is still active. */
  void parseHeartBeat(IMessage<K, M> message) {
    ControlMessage content = (ControlMessage)((Message<K, M>)message).getControlInfo();
    String heartBeat = new String(content.getExtraInfo());
    //System.out.println("Heartbeat = "+ heartBeat);
    if (!heartBeat.equals("10"))
      globalVoteToHalt = false;
  }

  /* Returns true if the peer is the master task, else false. */
  boolean isMasterTask(BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer) {
    return (getMasterTaskIndex() == peer.getPeerIndex()) ? true : false;
  }
  
  /* Peer 0 is the master task. */
  int getMasterTaskIndex() {
    return 0;
  }
 
  /* Sends message to the peer, which can later be parsed to reach the destination
   * e.g. subgraph, vertex etc. Also updates the messageInFlight boolean. */
  void sendMessage(String peerName, Message<K, M> message) {
    try {
      peer.send(peerName, message);
      if(message.getControlInfo().getTransmissionType() != IControlMessage.TransmissionType.HEARTBEAT) {
        messageInFlight = true;
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
 
  /* Sends message to all the peers. */
  void sendToAll(Message<K, M> message) {
    for (String peerName : peer.getAllPeerNames()) {
      sendMessage(peerName, message);
    }
  }
  
  void sendMessage(K subgraphID, M message) {
    Message<K, M> msg = new Message<K, M>(Message.MessageType.CUSTOM_MESSAGE, subgraphID, message);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.NORMAL);
    msg.setControlInfo(controlInfo);
    sendMessage(peer.getPeerName(subgraphPartitionMap.get(subgraphID)), msg);
  }
    
  void sendToVertex(I vertexID, M message) {
    //TODO
  }
 
  void sendToNeighbors(ISubgraph<S, V, E, I, J, K> subgraph, M message) {
    Set<K> sent = new HashSet<K>();
    for (IRemoteVertex<V, E, I, J, K> remotevertices: subgraph.getRemoteVertices()) {
      K neighbourID = remotevertices.getSubgraphID();
      if (!sent.contains(neighbourID)) {
        sent.add(neighbourID);
        sendMessage(neighbourID, message);
      }
    }
  }
  
  void sendToAll(M message) {
    Message<K, M> msg = new Message<K, M>(Message.MessageType.CUSTOM_MESSAGE, message);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    msg.setControlInfo(controlInfo);
    sendToAll(msg);
  }
  
  long getSuperStepCount() {
    return peer.getSuperstepCount() - INITIALIZATION_SUPERSTEPS;
  }
  
  int getPartitionID(K subgraphID) {
    return 0;
  }
  
  /* Master task notifies all the peers to finish bsp as all the subgraphs across all the peers
   * have voted to halt and there is no message in flight. */
  void notifyJobEnd() {
    assert(isMasterTask(peer));
    Message<K, M> msg = new Message<K, M>();
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.GLOBAL_HALT);
    msg.setControlInfo(controlInfo);
    sendToAll(msg);
  }
}
