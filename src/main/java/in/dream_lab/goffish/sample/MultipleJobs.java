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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import in.dream_lab.goffish.SubgraphCompute;
import in.dream_lab.goffish.api.IMessage;

public class MultipleJobs extends
    SubgraphCompute<MapWritable, MapWritable, MapWritable, Text, LongWritable, LongWritable, LongWritable> {

  Map<String, String> jobInputMap = new HashMap<>();

  public MultipleJobs(String jobInfo) {
    String jobClasses = jobInfo.split("-")[0];
    String jobArgs = jobInfo.split("-")[1];
    String jobClassesArr[] = jobClasses.split(";");
    String jobArgsArr[] = jobArgs.split(";");

    // length-1 as it ends with a trailing ';'
    for (int i = 0; i < jobClassesArr.length - 1; i++) {
      jobInputMap.put(jobClassesArr[i],
          (jobArgsArr[i].equals("") ? null : jobArgsArr[i]));
    }
  }

  @Override
  public void compute(Collection<IMessage<LongWritable, Text>> messages) {
    // TODO Auto-generated method stub
    
  }

}
