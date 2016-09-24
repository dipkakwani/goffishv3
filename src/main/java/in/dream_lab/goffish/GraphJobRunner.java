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
    extends BSP<LongWritable, LongWritable, LongWritable, LongWritable, Text> {

  
  @Override
  public final void setup(
      BSPPeer<LongWritable, LongWritable, LongWritable, LongWritable, Text> peer)
      throws IOException, SyncException, InterruptedException {

  
    Map<Long, Vertex> vertexMap = new HashMap<Long, Vertex>();
    List<Vertex> verticesList = new ArrayList<Vertex>();
    Map<Vertex, List<Edge>> adjList = new HashMap<Vertex, List<Edge>>();
    
    KeyValuePair<LongWritable, LongWritable> pair;
    long numPeers = peer.getNumPeers();
    while ((pair = peer.readNext()) != null) {
      long sourceID = pair.getKey().get();
      long sinkID = pair.getValue().get();
      int targetSourcePeer = (int)(sourceID % numPeers);
      int targetSinkPeer = (int)(sinkID % numPeers);
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
    Map<Long, StringBuilder> message = new HashMap<Long, StringBuilder>();
    List<Vertex> _vertices = new ArrayList<Vertex>();     // Final list of vertices.
    vertexMap = new HashMap<Long, Vertex>();
    
    // Send vertices to their respective partitions.
    for (Vertex v : verticesList) {
      StringBuilder sb = message.get(v.getVertexID());
      if (sb == null) {
        sb = new StringBuilder();
        message.put(v.getVertexID(), sb);
        sb.append(Long.toString(v.getVertexID()) + ',');
      }
      for (Edge e : v.outEdges()) {
        Vertex sink = e.getSink();
        sb.append(Long.toString(sink.getVertexID())).append(",");
      }
      int targetPeer = (int)(v.getVertexID() % numPeers);
      if (targetPeer != peer.getPeerIndex()) {
        Text msg = new Text(sb.toString());
        peer.send(peer.getPeerName(targetPeer), msg);
      }
      else {    // Belongs to this partition
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
      Vertex v = new Vertex(vertexID, (int)(vertexID % numPeers));
      _vertices.add(v);
      for (int i = 1; i < msgStringArr.length; i++) {
        long sinkID = Long.parseLong(msgStringArr[i]);
        Vertex sink = vertexMap.get(sinkID);
        if (sink == null) {
          sink = new Vertex(sinkID, (int)(sinkID % numPeers));
          vertexMap.put(sinkID, sink);
        }
        Edge e = new Edge(v, sink);
        v.addEdge(e);
      }
    }
    Partition partition = new Partition(peer.getPeerIndex());
    formSubgraphs(_vertices, peer, partition);
    
    // Get subgraph IDs from neighbors
    for (Vertex v : _vertices) {
      if (v.getSubgraphID() != -1) {
        for (Edge e : v.outEdges()) {
          Vertex sink = e.getSink();
          msg = new Text(sink.getVertexID() + "," + v.getVertexID() + "," + v.getSubgraphID());
          peer.send(peer.getPeerName((int)(sink.getVertexID() % peer.getNumPeers())), msg);
        }
      }
    }
    peer.sync();
    
    while ((msg = peer.getCurrentMessage()) != null) {
      String msgString = msg.toString();
      String msgStringArr[] = msgString.split(",");
      Long sinkID = Long.parseLong(msgStringArr[0]);
      Long sourceID = Long.parseLong(msgStringArr[1]);
      for (Subgraph subgraph : partition.getSubgraphs()) {
        Vertex sink = subgraph.getVertexByID(sinkID);
        if (sink != null) {
          int sourcePartiitionID = (int)(sourceID % peer.getNumPeers());
          peer.send(peer.getPeerName(sourcePartiitionID), 
              new Text(sink.getVertexID() + "," + sink.getSubgraphID() + "," + msgStringArr[2]));
        }
      }
    }
    
    peer.sync();
    
    while ((msg = peer.getCurrentMessage()) != null) {
      String msgString = msg.toString();
      String msgStringArr[] = msgString.split(",");
      Long sinkID = Long.parseLong(msgStringArr[0]);
      Long remoteSubgraphID = Long.parseLong(msgStringArr[1]);
      Long subgraphID = Long.parseLong(msgStringArr[2]);
      Subgraph subgraph = partition.getSubgraph(subgraphID);
      Vertex sink = subgraph.getVertexByID(sinkID);
      sink.setRemoteSubgraphID(remoteSubgraphID);
      
    }
  }
  
  /* Forms subgraphs by finding (weakly) connected components. */
  void formSubgraphs(List<Vertex> vertices,
      BSPPeer<LongWritable, LongWritable, LongWritable, LongWritable, Text> peer, Partition partition) {
    long subgraphCount = 0;
    Set<Long> visited = new HashSet<Long>();
    
    for (Vertex v : vertices) {
      if (!visited.contains(v.getVertexID())) {
        long subgraphID = subgraphCount++ | (((long)partition.getPartitionID()) << 32);
        Subgraph subgraph = new VertexCount(subgraphID, peer);
        partition.addSubgraph(subgraph);
        dfs(v, visited, subgraph, peer);
      }
    }
  }
  
  void dfs(Vertex v, Set<Long> visited, Subgraph subgraph,
      BSPPeer<LongWritable, LongWritable, LongWritable, LongWritable, Text> peer) {
    long vertexID = v.getVertexID();
    if (peer.getPeerIndex() == (int)(vertexID % peer.getNumPeers()))
      v.setSubgraphID(subgraph.getSubgraphID());
    else
      v.setSubgraphID(-1);
    subgraph.addVertex(v);
    visited.add(v.getVertexID());
    for (Edge e : v.outEdges()) {
      subgraph.addEdge(e);
      Vertex sink = e.getSink();
      if (!visited.contains(sink.getVertexID())) {
        dfs(sink, visited, subgraph, peer);
      }
    }
  }
  
  @Override
  public final void bsp(
      BSPPeer<LongWritable, LongWritable, LongWritable, LongWritable, Text> peer)
      throws IOException, SyncException, InterruptedException {
    
    /*TODO: Make execute subgraphs compute in parallel.
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors
        .newCachedThreadPool();
    executor.setMaximumPoolSize(64);*/
    Partition partition = new Partition(peer.getPeerIndex());
    boolean allVotedToHalt = false;
    while (!allVotedToHalt) {
      allVotedToHalt = true;
      List<Text> messages = new ArrayList<Text>();
      Text msg;
      while ((msg = peer.getCurrentMessage()) != null) {
        messages.add(msg);
      }

      for (Subgraph subgraph : partition.getSubgraphs()) {
        if (!subgraph.hasVotedToHalt()) {
          allVotedToHalt = false;
          subgraph.compute(messages);
        }
      }
    }

  }

  @Override
  public final void cleanup(
      BSPPeer<LongWritable, LongWritable, LongWritable, LongWritable, Text> peer)
      throws IOException {
    
  }
}
