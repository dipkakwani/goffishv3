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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.BSPJob.JobState;
import org.apache.hama.bsp.PartitioningRunner.RecordConverter;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.message.OutgoingMessageManager;
import org.apache.hama.bsp.message.queue.MessageQueue;

import com.google.common.base.Preconditions;

public class GraphJob extends BSPJob {
  
  public final static String VERTEX_CLASS_ATTR = "hama.subgraph.class";
  
  public GraphJob(HamaConfiguration conf, Class<? extends Subgraph> exampleClass)
      throws IOException {
    super(conf); 
    conf.setBoolean(Constants.ENABLE_RUNTIME_PARTITIONING, false);
    conf.setBoolean("hama.use.unsafeserialization", true);
    this.setBspClass(GraphJobRunner.class);
    this.setJarByClass(exampleClass);
    this.setPartitioner(HashPartitioner.class);
  }
  
  
  
  @Override
  public void setPartitioner(
      @SuppressWarnings("rawtypes") Class<? extends Partitioner> theClass) {
    super.setPartitioner(theClass);
  }
  
  /**
   * Set the Subgraph class for the job.
   */
  public void setSubgraphComputeClass(
      Class<? extends SubgraphCompute<? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable>> cls)
      throws IllegalStateException {
    conf.setClass(VERTEX_CLASS_ATTR, cls, Vertex.class);
    setInputKeyClass(cls);
    setInputValueClass(NullWritable.class);
  }
  
  /**
   * Sets the input reader for parsing the input to vertices.
   */
  public void setInputReaderClass(
      @SuppressWarnings("rawtypes") Class<? extends IReader> cls) {
    conf.setClass(Constants.RUNTIME_PARTITION_RECORDCONVERTER, cls,
        IReader.class);
  }
  
  public void setMaxIteration(int maxIteration) {
	    conf.setInt("hama.graph.max.iteration", maxIteration);
	}

  @Override
  public void submit() throws IOException, InterruptedException {
    super.submit();
  }

}
