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
import in.dream_lab.goffish.api.IRemoteVertex;

public class MetaGraph extends
    SubgraphCompute<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> {

  @Override
  public void compute(
      Collection<IMessage<LongWritable, LongWritable>> messages) {
    if (getSuperstep() == 0) {
      long sid = getSubgraph().getSubgraphId().get();
      for (IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph()
          .getRemoteVertices()) {
        System.out.println(sid + " " + vertex.getSubgraphId());
      }
      voteToHalt();
    }

  }
}
