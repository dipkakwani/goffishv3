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

import java.util.Collection;

import org.apache.hadoop.io.LongWritable;

import in.dream_lab.goffish.SubgraphCompute;
import in.dream_lab.goffish.api.IMessage;

/**
 * Prints the total number of vertices in the graph across all nodes
 * @author humus
 *
 */

public class VertexCount extends
    SubgraphCompute<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> {

  @Override
  public void compute(
      Collection<IMessage<LongWritable, LongWritable>> messages) {
    if (getSuperstep() == 0) {
      long count = getSubgraph().getLocalVertexCount();
      LongWritable message = new LongWritable(count);
      sendToAll(message);

    } else {

      long totalVertices = 0;
      for (IMessage<LongWritable, LongWritable> msg : messages) {
        LongWritable count = msg.getMessage();
        totalVertices += count.get();
      }
      System.out.println("Total vertices = " + totalVertices);
      try {
        // Use OutputWriter
      } finally {

      }
    }
    voteToHalt();
  }

}
