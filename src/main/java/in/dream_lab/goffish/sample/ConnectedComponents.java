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

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;

import in.dream_lab.goffish.GraphJob;
import in.dream_lab.goffish.SubgraphCompute;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;

public class ConnectedComponents {
  public static class CC extends
      SubgraphCompute<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> {

    long minSubgraphID;
    
    @Override
    public void compute(Collection<IMessage<LongWritable,LongWritable>> messages) {
      if (getSuperStep() == 0) {
        minSubgraphID = getSubgraph().getSubgraphID().get();       
        for (IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph()
            .getRemoteVertices()) {
          if (minSubgraphID > vertex.getSubgraphID().get()) {
            minSubgraphID = vertex.getSubgraphID().get();
          }
        }
        LongWritable msg = new LongWritable(minSubgraphID);
        sendToNeighbors(msg);
        getSubgraph().setValue(new LongWritable(minSubgraphID));
      } else {
        boolean updated = false;
        for (IMessage<LongWritable, LongWritable> msg : messages) {
          long subgraphID = msg.getMessage().get();
          if (minSubgraphID > subgraphID) {
            minSubgraphID = subgraphID;
            updated = true;
          }
        }
        if (updated) {
          getSubgraph().setValue(new LongWritable(minSubgraphID));
          LongWritable msg = new LongWritable(minSubgraphID);
          sendToNeighbors(msg);
        }
        System.out.println("Superstep "+ getSuperStep() + " Subgraph " + getSubgraph().getSubgraphID() + " value " + getSubgraph().getValue());
      }
      System.out.println("Subgraph " + getSubgraph().getSubgraphID() + " in superstep " + getSuperStep() + " value " + getSubgraph().getValue());
      voteToHalt();
    }
  
  
  }
  public static void main(String args[]) throws IOException,InterruptedException, ClassNotFoundException, ParseException
  {
	  HamaConfiguration conf = new HamaConfiguration();
	  GraphJob pageJob = new GraphJob(conf, CC.class);
	  pageJob.setJobName("Vertex Count");
	  pageJob.setInputFormat(TextInputFormat.class);
	  pageJob.setInputKeyClass(LongWritable.class);
	  pageJob.setInputValueClass(LongWritable.class);
	  pageJob.setOutputFormat(TextOutputFormat.class);
	  pageJob.setOutputKeyClass(LongWritable.class);
	  pageJob.setOutputValueClass(LongWritable.class);
	  pageJob.setMaxIteration(2);
	  pageJob.setInputPath(new Path(args[0]));
	  pageJob.setOutputPath(new Path(args[1]));
	  pageJob.waitForCompletion(true);
  }
}