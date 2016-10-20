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
import java.util.Collection;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;

public class VertexCount {
  public static class VrtxCnt extends
      Subgraph<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> {
    /* FIXME: Shouldn't be in user's logic. */
    VrtxCnt(int partitionID,LongWritable subgraphID) {
      super(partitionID,subgraphID);
      System.out.println("Application constructor called");
    }

    @Override
    public void compute(Collection<IMessage<LongWritable,LongWritable>> messages) {
      if (getSuperStep() == 0) {
        long count = 0;
        for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> v : subgraph.getLocalVertices()) {
          count++;
        }
        System.out.println("Number of local vertices = " + count);
        try {
          Text message = new Text(new Long(count).toString());
          for (String peers : peer.getAllPeerNames()) {
            peer.send(peers, message);
          }
        } catch (IOException e) {
        }
      } else {
        long totalVertices = 0;
        for (Text msg : messages) {
          String msgString = msg.toString();
          totalVertices += Long.parseLong(msgString);
        }
        System.out.println("Total vertices = " + totalVertices);
        try {
          peer.write(new LongWritable(getSubgraphID()),
              new LongWritable(totalVertices));
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      voteToHalt();
    }
  
  
  }
  public static void main(String args[]) throws IOException,InterruptedException, ClassNotFoundException, ParseException
  {
	  HamaConfiguration conf = new HamaConfiguration();
	  GraphJob pageJob = new GraphJob(conf, VrtxCnt.class);
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
