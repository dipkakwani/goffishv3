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

public class VertexCount {
  public static class VrtxCnt extends
      SubgraphCompute<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> {

    @Override
    public void compute(Collection<IMessage<LongWritable,LongWritable>> messages) {
      if (getSuperStep() == 0) {
        long count = getSubgraph().localVertexCount();
        System.out.println("Number of local vertices = " + count);

        LongWritable message = new LongWritable(count);
        sendToAll(message);

      } else {
        long totalVertices = 0;
        for (IMessage<LongWritable, LongWritable> msg : messages) {
          LongWritable count= msg.getMessage();
          totalVertices += count.get();
        }
        System.out.println("Total vertices = " + totalVertices);
        try {
          //Use OutputWriter
        }
        finally {
          
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
