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

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;

import in.dream_lab.goffish.GraphJob;
import in.dream_lab.goffish.PartitionsLongTextAdjacencyListReader;


public class EdgeListJob {
  public static void main(String args[]) throws IOException,InterruptedException, ClassNotFoundException, ParseException
  {
          HamaConfiguration conf = new HamaConfiguration();
          GraphJob job = new GraphJob(conf, EdgeList.class);
          job.setJobName("Edge List");
          job.setInputFormat(TextInputFormat.class);
          job.setInputKeyClass(LongWritable.class);
          job.setInputValueClass(LongWritable.class);
          job.setOutputFormat(TextOutputFormat.class);
          job.setOutputKeyClass(LongWritable.class);
          job.setOutputValueClass(LongWritable.class);
          job.setInputPath(new Path(args[0]));
          job.setOutputPath(new Path(args[1]));
          job.setGraphMessageClass(LongWritable.class);
          
          /* Reader configuration */
          // job.setInputFormat(NonSplitTextInputFormat.class);
          // job.setInputFormat(TextInputFormat.class);
          job.setInputReaderClass(PartitionsLongTextAdjacencyListReader.class);
          
          //blocks till job completed
          job.waitForCompletion(true);
  }
}
