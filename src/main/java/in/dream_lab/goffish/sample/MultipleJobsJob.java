/*
 *      Copyright 2017 DREAM:Lab, Indian Institute of Science, Bangalore
 *
 *      Licensed under the Apache License, Version 2.0 (the "License"); you may
 *      not use this file except in compliance with the License. You may obtain
 *      a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

package in.dream_lab.goffish.sample;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import in.dream_lab.goffish.GraphJob;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;

public class MultipleJobsJob {
  
  public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
    //NOt yet done
    HamaConfiguration conf = new HamaConfiguration();
    GraphJob job = new GraphJob(conf, PageRank.class);
    job.setJobName("Multiple Jobs");
    job.setInputFormat(TextInputFormat.class);
    job.setInputKeyClass(LongWritable.class);
    job.setInputValueClass(LongWritable.class);
    job.setOutputFormat(TextOutputFormat.class);
    job.setInputPath(new Path(args[0]));
    job.setOutputPath(new Path(args[1]));
    job.setGraphMessageClass(Text.class);
    
    String jobClasses = "";
    String jobArgs = "";
    if (args[2].equals("-f")) {
      //take input from a file
      BufferedReader fileReader = new BufferedReader(new FileReader(args[3]));
      String jobInfo = "";
      while((jobInfo = fileReader.readLine()) != null) {
        String jobInfoArr[] = jobInfo.trim().split("\\s+");
        jobClasses += jobInfoArr[0] + ";";
        //if job has no arguments just add another ; with no value
        jobArgs += ((jobInfoArr.length > 1) ? (jobInfoArr[1] +";") : ";");
      }
    }
    else {
      //take from command line
    }
    
    job.setInitialInput(jobClasses + "-" + jobArgs);
    
    //blocks till job completed
    job.waitForCompletion(true);
  }
}
