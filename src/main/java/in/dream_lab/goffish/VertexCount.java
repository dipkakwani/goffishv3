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
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPPeer;

public class VertexCount extends Subgraph {
  
  void compute(List<Text> messages) {
    if (getSuperStep() == 0) {
      long count = 0;
      for (Vertex v : getVertices()) {
        if (!v.isRemote()) {
          count++;
        }
      }
      try{
        Text message=new Text(new Long(count).toString());
        for (String peers:peer.getAllPeerNames()){
          peer.send(peers, message);
        }
      }
      catch(IOException e)
      {}
    }
    else {
      long totalVertices=0;
      for(Text msg:messages){
        String msgString = msg.toString();
        totalVertices+=Long.parseLong(msgString);
      }
      try {
          peer.write(new LongWritable(getSubgraphID()), new LongWritable(totalVertices));
	} catch (IOException e) {
	  // TODO Auto-generated catch block
	  e.printStackTrace();
	}
    }
    voteToHalt();
  }
  
  /*FIXME: Shouldn't be in user's logic. */
  VertexCount(long subgraphID, 
      BSPPeer<LongWritable, LongWritable, LongWritable, LongWritable, Text> peer) {
    super(subgraphID,  peer);
  }
}
