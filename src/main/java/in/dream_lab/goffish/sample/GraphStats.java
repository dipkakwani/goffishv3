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

package in.dream_lab.goffish.sample;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Iterables;

import in.dream_lab.goffish.SubgraphCompute;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.ISubgraph;

public class GraphStats extends
    SubgraphCompute<LongWritable, LongWritable, LongWritable, Text, LongWritable, LongWritable, LongWritable> {

  private long _vertexCount = 0;
  private long _edgeCount = 0;
  private long _subgraphCount = 0;
  private long _metaGraphDiameter = 1;
  private Map<Long,Long> _distanceMap = new HashMap<>();
  private List<Long> _probed = new ArrayList<>();
  
  @Override
  public void compute(
      Collection<IMessage<LongWritable, Text>> messages) {
    // TODO Auto-generated method stub
    /*
     * Vertex count,
     * edge count,
     * number of subgraphs.
     * diameter (meta graph and actual graph),
     * each subgraph vertex and edge (local and remote) count,
     */
    
    if (getSuperStep() == 0) {
      long vertexCount = getSubgraph().localVertexCount();
      long edgeCount = Iterables.size(getSubgraph().getEdges());
      String msgString = vertexCount + ";" + edgeCount;
      Text message = new Text(msgString);
      sendToAll(message);
      return;
    }
    
    if (getSuperStep() == 1) {
      for (IMessage<LongWritable, Text> message : messages) {
        //number of messages recieved = number of subgraphs
        _subgraphCount++;
        String msgString = message.getMessage().toString();
        String msgStringarr[] = msgString.split(";");
        _vertexCount += Long.parseLong(msgStringarr[0]);
        _edgeCount += Long.parseLong(msgStringarr[1]);
      }
      System.out.println(_vertexCount + ";" + _edgeCount);
      return;
    }
    
    //Find Diameter of meta graph from this superstep
    
    if(getSuperStep() == 2) {
      //create a probe message(messageType;subgraphid;distance to next node)
      //P = Probe Message
      //R = Reply Message
      String msg = "P;"+getSubgraph().getSubgraphID().get() + ";"+2;
      _probed.add(getSubgraph().getSubgraphID().get());
      //distance to itself is 1
      _distanceMap.put(getSubgraph().getSubgraphID().get(), new Long(1));
      Text probeMessage = new Text (msg);
      sendToNeighbors(probeMessage);
      return;
    }
    
    //true if any probe forwarded by this subgraph (signifies the algo is progressing)
    boolean hasUpdates = false;
    //true if any other subgraph is still alive(same condition as above for that subgraph)
    boolean progressing = false;
    //when all values have been computed call the cleanup procedure to display output
    boolean callCleanup = false;
    
    for (IMessage<LongWritable, Text> recievedMessage : messages) {
      
      if (recievedMessage.getMessage().toString().equals("HasUpdates")) {
        progressing =true;
        continue;
      }
      
      if(recievedMessage.getMessage().charAt(0) == 'D') {
        //Diameter broadcast
        String msgArr[] = recievedMessage.getMessage().toString().split(";");
        Long recievedDiameter = Long.parseLong(msgArr[1]);
        if (recievedDiameter > _metaGraphDiameter) {
          _metaGraphDiameter = recievedDiameter;
        }
        callCleanup = true;
        continue;
      }
      
      String msg = recievedMessage.getMessage().toString();
      String msgArr[] = msg.split(";");
      Long subgraphID = Long.parseLong(msgArr[1]);
      Long distance = Long.parseLong(msgArr[2]);
      if (msgArr[0] == "P") {
        if (_probed.contains(subgraphID)) {
          //already probed
          continue;
        }
        hasUpdates = true;
        _probed.add(subgraphID);
        //forward the probe to neighbours
        String forwardProbeMsg = "P;"+subgraphID+";"+(distance+1);
        sendToNeighbors(new Text(forwardProbeMsg));
        //reply with the distance to the probe initiator
        String replyMsg = "R;"+getSubgraph().getSubgraphID().get()+";"+distance;
        sendMessage(new LongWritable(subgraphID), new Text(replyMsg));
      }
      //if this the Reply message add it to the map
      else {
        _distanceMap.put(subgraphID, distance);
      }
    }
    
    if (hasUpdates) {
      sendToAll(new Text("HasUpdates"));
    }
    
    if (callCleanup) {
      cleanup();
      voteToHalt();
      return;
    }
    
    //if values have converged
    if (!hasUpdates && !progressing) {
      //broadcast the max diameter to everyone
      for (Map.Entry<Long, Long> subgraphDistancepair : _distanceMap.entrySet()) {
        if (subgraphDistancepair.getValue() > _metaGraphDiameter) {
          _metaGraphDiameter = subgraphDistancepair.getValue();
        }
      }
      String msg = "D;"+_metaGraphDiameter;
      sendToAll(new Text(msg));
    }
    
    voteToHalt();
  }

  private void cleanup() {
    System.out.println("Vertex Count = "+_vertexCount);
    System.out.println("Edge Count = "+_edgeCount);
    System.out.println("Subgraph Count = "+_subgraphCount);
    System.out.println("Meta Graph Diameter = "+_metaGraphDiameter);
    ISubgraph<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
    System.out.println("Subgraph "+subgraph.getSubgraphID()+" has "+subgraph.localVertexCount() +" local vertices");
    System.out.println("Subgraph "+subgraph.getSubgraphID()+" has "+(subgraph.vertexCount() - subgraph.localVertexCount()) +" remote vertices");
    System.out.println("Subgraph "+subgraph.getSubgraphID()+" has "+Iterables.size(subgraph.getEdges()) +" edges");
  }

}
