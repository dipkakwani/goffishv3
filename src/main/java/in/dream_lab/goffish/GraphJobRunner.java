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
import in.dream_lab.goffish.VertexCount.VrtxCnt;

import org.apache.hama.util.ReflectionUtils;
import org.apache.hama.util.UnsafeByteArrayInputStream;
import org.apache.hama.util.WritableUtils;

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
  
  private Partition<S, V, E, I, J, K> partition;
  private BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private HamaConfiguration conf;
  private Map<K, Integer> subgraphPartitionMap;
  private static Class<?> SUBGRAPH_CLASS;
  //public static Class<Subgraph<?, ?, ?, ?, ?, ?, ?>> subgraphClass;
  private Map<K, List<IMessage<K, M>>> subgraphMessageMap;
  
  @Override
  public final void setup(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer)
      throws IOException, SyncException, InterruptedException {

    System.out.println("BSP Setup");
    setupfields(peer);
    /*TODO: Read input reader class type from Hama conf. 
     * FIXME:Make type of Message generic in Reader. */
    /*
    IReader<Writable, Writable, Writable, Writable, S, V, E, I, J, K> reader = 
        (IReader<Writable, Writable, Writable, Writable, S, V, E, I, J, K>) ReflectionUtils
        .newInstance(conf.getClass(Constants.RUNTIME_PARTITION_RECORDCONVERTER, LongTextAdjacencyListReader.class));
        */
    IReader<Writable, Writable, Writable, Writable, S, V, E, I, J, K> reader = 
        (IReader<Writable, Writable, Writable, Writable, S, V, E, I, J, K>)new LongTextAdjacencyListReader<S, V, E, K, M>(peer,subgraphPartitionMap);
    for (ISubgraph<S, V, E, I, J, K> subgraph: reader.getSubgraphs()) {
      partition.addSubgraph(subgraph);
    }
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
    System.out.println("BSP method at superstep "+peer.getSuperstepCount());
    
    /*
     * Creating SubgraphCompute objects
     */
    List<SubgraphCompute<S, V, E, M, I, J, K>> subgraphs=new ArrayList<SubgraphCompute<S, V, E, M, I, J, K>>();
    for (ISubgraph<S, V, E, I, J, K> subgraph : partition.getSubgraphs()) {
      
      /* FIXME: Read generic types from configuration and make subgraph object generic. */
      VertexCount.VrtxCnt subgraphComputeRunner = new VertexCount.VrtxCnt();
      subgraphComputeRunner.init((GraphJobRunner<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) this);
      subgraphComputeRunner.setSubgraph((ISubgraph<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>)subgraph);
      subgraphs.add((SubgraphCompute<S, V, E, M, I, J, K>) subgraphComputeRunner);
    }
    boolean allVotedToHalt = false;
    while (!allVotedToHalt) {
      allVotedToHalt = true;
      List<IMessage<K, M>> messages = new ArrayList<IMessage<K, M>>();
      Message<K, M> msg;
      while ((msg = peer.getCurrentMessage()) != null) {
        messages.add(msg);
      }
      parseMessage(messages);

      for (SubgraphCompute<S, V, E, M, I, J, K> subgraph : subgraphs) {
        System.out.println("Calling compute with vertices"+subgraph.getSubgraph().localVertexCount());
        /*
         * TODO : Clean up the code to call subgraphs that receive
         * message even when halted
         * */
        List<IMessage<K, M>> messagesToSubgraph = subgraphMessageMap.get(subgraph.getSubgraph().getSubgraphID());
        if (!subgraph.hasVotedToHalt() || messagesToSubgraph.size()>0) {
          allVotedToHalt = false;
          subgraph.compute(messages);
        }
      }
      peer.sync();
    }

  }

  @Override
  public final void cleanup(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer)
      throws IOException {
    /*TODO: Call reduce of the application. */
  }

  void parseMessage(List<IMessage<K, M>> messages) {
    for (IMessage<K, M> message : messages) {
      //Broadcase message, therefore every subgraph receives it
      if(((Message<K, M>)message).getControlInfo().getTransmissionType() == IControlMessage.TransmissionType.BROADCAST) {
        for (ISubgraph<S, V, E, I, J, K> subgraph : partition.getSubgraphs()) {
          List<IMessage<K, M>> subgraphMessage = subgraphMessageMap.get(subgraph.getSubgraphID());
          if(subgraphMessage == null) {
            subgraphMessage = new ArrayList<IMessage<K, M>>();
          }
          subgraphMessage.add(message);
        }
      }
      else if(((Message<K, M>)message).getControlInfo().getTransmissionType() == IControlMessage.TransmissionType.NORMAL) {
        List<IMessage<K, M>> subgraphMessage = subgraphMessageMap.get(message.getSubgraphID());
        if(subgraphMessage == null) {
          subgraphMessage = new ArrayList<IMessage<K, M>>();
        }
        subgraphMessage.add(message);
      }
      /*
       * TODO: Add implementation for partition message and vertex message(used for graph mutation)
       */
    }
  }

  void sendMessage(K subgraphID, M message) {
    //List<Message<K, M>> messages = _messages.get(subgraphID);
    //if (messages == null) {
    //  messages = new ArrayList<Message<K, M>>();
    //}
    Message<K, M> msg = new Message<K, M>(Message.MessageType.CUSTOM_MESSAGE, subgraphID, message);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.NORMAL);
    msg.setControlInfo(controlInfo);
    try {
      peer.send(peer.getPeerName(subgraphPartitionMap.get(subgraphID)), msg);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    //messages.add(msg);
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
    //_broadcastMessages.add(msg);
    for (String peerName : peer.getAllPeerNames()) {
      try {
        peer.send(peerName, msg);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
  long getSuperStepCount() {
    return peer.getSuperstepCount()-INITIALIZATION_SUPERSTEPS;
  }
  
  int getPartitionID(K subgraphID) {
    return 0;
  }
}
