package com.ankus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ankus.TaggedGroupKeyComparator;


public class MainDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    
    int res = ToolRunner.run(new Configuration(), new MainDriver(), args);
    System.out.println("MR-Job Result:" + res);
  }
  private static final String MNET_OUTPUT = "MNET_OUTPUT";
  
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

    
    if (otherArgs.length != 3) {
      System.err.println("Usage: MainDriver <Mnet> <Melon> <Gaon>");
      System.exit(2);
    }


//-------------------------------------------------------------------------------------------------------------------------------
//  
//		Pre Job Operating Part1 - Mnet information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
			Job job1 = new Job(getConf(), "TaxArrangeDriver");
	 
			job1.setJarByClass(MainDriver.class);
	    
	    
			FileOutputFormat.setOutputPath(job1, new Path(MNET_OUTPUT));

	    
	    
			
			//job1.setReducerClass(TaxReducer.class);
	    
	    
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);
	   
			job1.setInputFormatClass(TextInputFormat.class);
			
			job1.setOutputFormatClass(TextOutputFormat.class);
	    
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			MultipleInputs.addInputPath(job1, new Path(otherArgs[3]),
				      TextInputFormat.class, MnetMapper.class);
			job1.waitForCompletion(true);
			

			
			return job1.waitForCompletion(true) ? 0:1;
			
			
  }
}