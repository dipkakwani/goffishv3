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
public final class GraphJobRunner
    extends BSP<LongWritable, Text, LongWritable, LongWritable, Text> {

  Partition partition;
  BSPPeer<LongWritable, Text, LongWritable, LongWritable, Text> peer;
  
  int getPartitionID(Vertex v){
    return (int) v.getVertexID() % peer.getNumPeers();
  }
  
  int getPartitionID(long vertexID){
    return (int) vertexID % peer.getNumPeers();
  }
  
  @Override
  public final void setup(
      BSPPeer<LongWritable, Text, LongWritable, LongWritable, Text> peer)
      throws IOException, SyncException, InterruptedException {

    setupfields(peer);
    Map<Long, Vertex> vertexMap = new HashMap<Long, Vertex>();
    List<Vertex> verticesList = new ArrayList<Vertex>();
    
    KeyValuePair<LongWritable, Text> pair;
    long numPeers = peer.getNumPeers();
    while ((pair = peer.readNext()) != null) {
      long sourceID = pair.getKey().get();
      String value[] = pair.getValue().toString().split("\t");
      // long sourceID = Long.parseLong(value[0]);
      String edgeList[] = value[1].split(" ");
      int targetSourcePeer = (int) (sourceID % numPeers);
      for (String dest : edgeList) {
        long sinkID = Long.parseLong(dest);
        int targetSinkPeer = (int) (sinkID % numPeers);
        Vertex source = vertexMap.get(sourceID);
        Vertex sink = vertexMap.get(sinkID);
        if (source == null) {
          source = new Vertex(sourceID, targetSourcePeer);
          vertexMap.put(sourceID, source);
          verticesList.add(source);
        }
        if (sink == null) {
          sink = new Vertex(sinkID, targetSinkPeer);
          vertexMap.put(sinkID, sink);
          verticesList.add(source);
        }
        Edge e = new Edge(source, sink);
        source.addEdge(e);
      }
    }
    List<Vertex> _vertices = new ArrayList<Vertex>(); // Final list of vertices.
    vertexMap = new HashMap<Long, Vertex>();

    // Send vertices to their respective partitions.
    for (Vertex v : verticesList) {
      int targetPeer = getPartitionID(v);
      if (targetPeer != peer.getPeerIndex()) {
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toString(v.getVertexID()));
        for (Edge e : v.outEdges()) {
          Vertex sink = e.getSink();
          sb.append(",").append(Long.toString(sink.getVertexID()));
        }
        Text msg = new Text(sb.toString());
        peer.send(peer.getPeerName(targetPeer), msg);
      } else { // Belongs to this partition
        _vertices.add(v);
        vertexMap.put(v.getVertexID(), v);
      }
    }

    // End of first superstep.
    peer.sync();
    
    Text msg;
    while ((msg = peer.getCurrentMessage()) != null) {
      String msgString = msg.toString();
      String msgStringArr[] = msgString.split(",");
      long vertexID = Long.parseLong(msgStringArr[0]);
      Vertex v = new Vertex(vertexID, getPartitionID(vertexID));
      _vertices.add(v);
      vertexMap.put(v.getVertexID(), v);
      for (int i = 1; i < msgStringArr.length; i++) {
        long sinkID = Long.valueOf(msgStringArr[i]);
        Vertex sink = vertexMap.get(sinkID);
        if (sink == null) {
          sink = new Vertex(sinkID, (int)(sinkID % numPeers));
          vertexMap.put(sinkID, sink);
        }
        Edge e = new Edge(v, sink);
        v.addEdge(e);
      }
    }
    formSubgraphs(_vertices);

    /*
     * Ask Remote Vertices to send their subgraph IDs. Requires 2 supersteps
     * because the graph is directed
     */
    for (Vertex v : _vertices) {
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
      for (Vertex v : _vertices) {
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
      for(Vertex v : _vertices) {
        if (v.getVertexID() == sinkID) {
          v.setRemoteSubgraphID(remoteSubgraphID);
        }
      }
    }
  }
  
  /*Initialize the  fields*/
  private void setupfields(
      BSPPeer<LongWritable, Text, LongWritable, LongWritable, Text> peer) {
    
    this.peer = peer;
    partition = new Partition(peer.getPeerIndex());
    
  }

  /* Forms subgraphs by finding (weakly) connected components. */
  void formSubgraphs(List<Vertex> vertices) {
    long subgraphCount = 0;
    Set<Long> visited = new HashSet<Long>();
    
    for (Vertex v : vertices) {
      if (!visited.contains(v.getVertexID())) {
        long subgraphID = subgraphCount++ | (((long)partition.getPartitionID()) << 32);
        Subgraph subgraph = new VertexCount.VrtxCnt(subgraphID, peer);
        System.out.println(subgraph.getSubgraphID());
        partition.addSubgraph(subgraph);
        dfs(v, visited, subgraph);
      }
    }
  }
  
  void dfs(Vertex v, Set<Long> visited, Subgraph subgraph) {
    if (peer.getPeerIndex() == getPartitionID(v)) {
      v.setSubgraphID(subgraph.getSubgraphID());
      subgraph.addLocalVertex(v);
    } else {
      v.setSubgraphID(-1);
      subgraph.addRemoteVertex(v);
    }
    subgraph.addVertex(v);
    System.out.println("Adding Vertex" + subgraph.vertexCount());
    visited.add(v.getVertexID());
    for (Edge e : v.outEdges()) {
      subgraph.addEdge(e);
      Vertex sink = e.getSink();
      if (!visited.contains(sink.getVertexID())) {
        dfs(sink, visited, subgraph);
      }
    }
  }
  
  @Override
  public final void bsp(
      BSPPeer<LongWritable, Text, LongWritable, LongWritable, Text> peer)
      throws IOException, SyncException, InterruptedException {
    
    /*TODO: Make execute subgraphs compute in parallel.
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors
        .newCachedThreadPool();
    executor.setMaximumPoolSize(64);*/
    System.out.println("BSP method");
    boolean allVotedToHalt = false;
    while (!allVotedToHalt) {
      allVotedToHalt = true;
      List<Text> messages = new ArrayList<Text>();
      Text msg;
      while ((msg = peer.getCurrentMessage()) != null) {
        messages.add(msg);
      }

      System.out.println("Getting all subgraphs in partition");
      for (Subgraph subgraph : partition.getSubgraphs()) {
        System.out.println("calling compute"+subgraph.vertexCount());
        if (!subgraph.hasVotedToHalt()) {
          allVotedToHalt = false;
          subgraph.compute(messages);
        }
      }
      peer.sync();
    }

  }

  @Override
  public final void cleanup(
      BSPPeer<LongWritable, Text, LongWritable, LongWritable, Text> peer)
      throws IOException {
    
  }
}
