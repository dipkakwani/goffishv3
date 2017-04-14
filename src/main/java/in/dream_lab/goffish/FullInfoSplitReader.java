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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;

import com.google.common.primitives.Longs;

import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.humus.api.IControlMessage;
import in.dream_lab.goffish.humus.api.IReader;

/**
 * 
 * @author humus
 *
 * @param <S>
 * @param <V>
 * @param <E>
 * @param <K>
 * @param <M>
 *
 * Expected format :
 * pid sgid     vid sinkid1 sgid1 pid1 sinkid2 sgid2 pid2 ...
 * 
 * As this reader takes split files it might also get those vertices that belong to other partitions
 * superstep 1 - shuffles the vertices around and send them to their respective partition(creates a few obj also)
 * superstep 2 - create objects(vertex,edge and remote vertex). send the subgraphIDs that we have to all other partitions.
 * Superstep 3 - generate subgraphPartitionMapping from the incoming msgs. 
 */
public class FullInfoSplitReader<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable>
    implements
    IReader<Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {

  public static final Log LOG = LogFactory.getLog(FullInfoSplitReader.class);

  private HamaConfiguration conf;
  private BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition;
  private Map<K, Integer> subgraphPartitionMap;
  private int edgeCount = 0;

  public FullInfoSplitReader(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer,
      Map<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
    this.conf = peer.getConfiguration();
    partition = new Partition<>(peer.getPeerIndex());
  }

  @Override
  public List<ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {

    KeyValuePair<Writable, Writable> pair;
    while ((pair = peer.readNext()) != null) {
      String stringInput = pair.getValue().toString();
      // pid is the first column and its range is 0 to max pid
      int partitionID = Integer
          .parseInt(stringInput.substring(0, stringInput.indexOf(' ')));
      LOG.debug("partitionID = "+partitionID);
      
      if (partitionID != peer.getPeerIndex()) {
        // send vertex to its correct partition
        Message<K, M> msg = new Message<>();
        msg.setMessageType(IMessage.MessageType.VERTEX);
        ControlMessage ctrl = new ControlMessage();
        ctrl.setTransmissionType(IControlMessage.TransmissionType.VERTEX);
        ctrl.addextraInfo(stringInput.getBytes());
        msg.setControlInfo(ctrl);
        peer.send(peer.getPeerName(partitionID), msg);

      } else {

        // belongs to this partition
        createVertex(stringInput);
      }
    }

    peer.sync();

    Message<K, M> msg;
    //recieve all incoming vertices
    while ((msg = peer.getCurrentMessage()) != null) {
      ControlMessage receivedCtrl = (ControlMessage) msg.getControlInfo();
      createVertex(new String(
          receivedCtrl.getExtraInfo().iterator().next().copyBytes()));
    }

    // broadcast all subgraphs belonging to this partition
    Message<K, M> subgraphMapppingMessage = new Message<>();
    subgraphMapppingMessage.setMessageType(IMessage.MessageType.CUSTOM_MESSAGE);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    controlInfo.setPartitionID(peer.getPeerIndex());
    subgraphMapppingMessage.setControlInfo(controlInfo);
    for (ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph : partition
        .getSubgraphs()) {

      byte subgraphIDbytes[] = Longs
          .toByteArray(subgraph.getSubgraphId().get());
      controlInfo.addextraInfo(subgraphIDbytes);
    }

    sendToAllPartitions(subgraphMapppingMessage);

    peer.sync();
    Message<K, M> subgraphMappingInfoMessage;
    while ((subgraphMappingInfoMessage = peer.getCurrentMessage()) != null) {
      ControlMessage receivedCtrl = (ControlMessage) subgraphMappingInfoMessage.getControlInfo();
      Integer partitionID = receivedCtrl.getPartitionID();
      for (BytesWritable rawSubgraphID : receivedCtrl.getExtraInfo()) {
        LongWritable subgraphID = new LongWritable(
            Longs.fromByteArray(rawSubgraphID.copyBytes()));
        subgraphPartitionMap.put((K) subgraphID, partitionID);
      }
    }

    return partition.getSubgraphs();
  }

  private void sendToAllPartitions(Message<K, M> message) throws IOException {
    for (String peerName : peer.getAllPeerNames()) {
      peer.send(peerName, message);
    }
  }

  private void createVertex(String stringInput) {

    // belongs to this partition
    String vertexValue[] = stringInput.split("\\s+");

    LongWritable vertexID = new LongWritable(
        Long.parseLong(vertexValue[1]));
    int partitionID = Integer.parseInt(vertexValue[0]);
    LongWritable vertexSubgraphID = new LongWritable(
        Long.parseLong(vertexValue[2]));

    Subgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph = (Subgraph<S, V, E, LongWritable, LongWritable, LongWritable>) partition
        .getSubgraph(vertexSubgraphID);

    if (subgraph == null) {
      subgraph = new Subgraph<S, V, E, LongWritable, LongWritable, LongWritable>(
          partitionID, vertexSubgraphID);
      partition.addSubgraph(subgraph);
    }
    Vertex<V, E, LongWritable, LongWritable> vertex = (Vertex<V, E, LongWritable, LongWritable>) subgraph
        .getVertexById(vertexID);
    if (vertex == null) {
      // vertex not added already
      vertex = new Vertex<V, E, LongWritable, LongWritable>(vertexID);
      subgraph.addVertex(vertex);
    }

    for (int j = 3; j < vertexValue.length; j++) {
      if (j + 3 > vertexValue.length) {
        LOG.debug("Incorrect length of line for vertex " + vertexID);
      }
      LongWritable sinkID = new LongWritable(Long.parseLong(vertexValue[j]));
      LongWritable sinkSubgraphID = new LongWritable(
          Long.parseLong(vertexValue[j + 1]));
      int sinkPartitionID = Integer.parseInt(vertexValue[j + 2]);
      j += 2;
      LongWritable edgeID = new LongWritable(
          edgeCount++ | (((long) peer.getPeerIndex()) << 32));
      Edge<E, LongWritable, LongWritable> e = new Edge<E, LongWritable, LongWritable>(
          edgeID, sinkID);
      vertex.addEdge(e);
      if (subgraph.getVertexById(sinkID) != null) {
        // If vertex already created its already added to its correct
        // subgraph
        continue;
      }
      // If sink does not exist create new one
      if (sinkPartitionID != peer.getPeerIndex()) {
        // this is a remote vertex
        IRemoteVertex<V, E, LongWritable, LongWritable, LongWritable> sink = new RemoteVertex<>(
            sinkID, sinkSubgraphID);
        // Add it to the same subgraph, as this is part of weakly connected
        // component
        subgraph.addVertex(sink);
      } else {
        Vertex<V, E, LongWritable, LongWritable> sink = new Vertex<>(sinkID);
        subgraph.addVertex(sink);
      }
    }
  }
}
