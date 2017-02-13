package in.dream_lab.goffish.sample;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;

import in.dream_lab.goffish.GraphJob;

public class PageRankJob {
  
  public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
    
    HamaConfiguration conf = new HamaConfiguration();
    GraphJob pageJob = new GraphJob(conf, PageRank.class);
    pageJob.setJobName("Page Rank");
    pageJob.setInputFormat(TextInputFormat.class);
    pageJob.setInputKeyClass(LongWritable.class);
    pageJob.setInputValueClass(LongWritable.class);
    pageJob.setOutputFormat(TextOutputFormat.class);
    pageJob.setOutputKeyClass(LongWritable.class);
    pageJob.setOutputValueClass(LongWritable.class);
    pageJob.setInputPath(new Path(args[0]));
    pageJob.setOutputPath(new Path(args[1]));
    pageJob.setGraphMessageClass(LongWritable.class);
    
    //blocks till job completed
    pageJob.waitForCompletion(true);
  }

}
