package in.dream_lab.goffish.sample;
/*
         *      Copyright 2014 DREAM:Lab, Indian Institute of Science, Bangalore
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
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;

import in.dream_lab.goffish.GraphJob;
import in.dream_lab.goffish.PartitionsLongTextAdjacencyListReader;

public class PageRankJob {
  
  public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
    
    HamaConfiguration conf = new HamaConfiguration();
    GraphJob job = new GraphJob(conf, PageRank.class);
    job.setJobName("Page Rank");
    job.setInputFormat(TextInputFormat.class);
    job.setInputKeyClass(LongWritable.class);
    job.setInputValueClass(LongWritable.class);
    job.setOutputFormat(TextOutputFormat.class);
    job.setInputPath(new Path(args[0]));
    job.setOutputPath(new Path(args[1]));
    job.setGraphMessageClass(Text.class);
    
    /* Reader configuration */
    // job.setInputFormat(NonSplitTextInputFormat.class);
    // job.setInputFormat(TextInputFormat.class);
    job.setInputReaderClass(PartitionsLongTextAdjacencyListReader.class);
    
    //blocks till job completed
    job.waitForCompletion(true);
  }

}
