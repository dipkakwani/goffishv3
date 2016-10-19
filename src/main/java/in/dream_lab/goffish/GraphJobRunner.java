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
    extends BSP<Writable, Writable, Writable, Writable, IMessage<K, M>> {

  Partition<S, V, E, I, J, K> partition;
  BSPPeer<Writable, Writable, Writable, Writable, IMessage<K, M>> peer;
  
  
  int getPartitionID(IVertex<V, E, I, J> v){
    return (int) v.getVertexID() % peer.getNumPeers();
  }
  
  int getPartitionID(long vertexID){
    return (int) vertexID % peer.getNumPeers();
  }
  
  @Override
  public final void setup(
      BSPPeer<Writable, Writable, Writable, Writable, IMessage<K, M>> peer)
      throws IOException, SyncException, InterruptedException {

    setupfields(peer);
    /*TODO: Read input reader class type from Hama conf. 
     * FIXME:Make type of IMessage generic in Reader. */
    LongTextAdjacencyListReader<S, V, E> reader = new LongTextAdjacencyListReader<S, V, E>(peer, partition);
    // TODO: get subgraphs.
  }
  
  /*Initialize the  fields*/
  private void setupfields(
      BSPPeer<Writable, Writable, Writable, Writable, IMessage<K, M>> peer) {
    
    this.peer = peer;
    partition = new Partition(peer.getPeerIndex());
    
  }

   
  @Override
  public final void bsp(
      BSPPeer<Writable, Writable, Writable, Writable, IMessage<K, M>> peer)
      throws IOException, SyncException, InterruptedException {
    
    /*TODO: Make execute subgraphs compute in parallel.
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors
        .newCachedThreadPool();
    executor.setMaximumPoolSize(64);*/
    System.out.println("BSP method at superstep "+peer.getSuperstepCount());
    boolean allVotedToHalt = false;
    while (!allVotedToHalt) {
      allVotedToHalt = true;
      List<Text> messages = new ArrayList<Text>();
      Text msg;
      while ((msg = peer.getCurrentMessage()) != null) {
        messages.add(msg);
      }

      /* FIXME: Read generic types from configuration and make subgraph object generic. */
      for (Subgraph subgraph : partition.getSubgraphs()) {
        System.out.println("Calling compute "+subgraph.localVertexCount());
        /*
         * TODO : Clean up the code to call subgraphs that receive
         * message even when halted
         * */
        if (!subgraph.hasVotedToHalt() || messages.size()>0) {
          allVotedToHalt = false;
          subgraph.compute(messages);
        }
      }
      peer.sync();
    }

  }

  @Override
  public final void cleanup(
      BSPPeer<Writable, Writable, Writable, Writable, IMessage<K, M>> peer)
      throws IOException {
    
  }
}
