package com.ankus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;


/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : MainDriver.java
* 3. 작성일 : 2017. 11. 20. 오전 1:25:57
* 4. 작성자 : mypc
* 5. 설명 : 크롤링 데이터 전처리 및 분석을 위한 메인 드라이버
* </pre>
*/
public class MainDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
   int res = ToolRunner.run(new Configuration(), new MainDriver(), args);
    System.out.println("MR-Job Result:" + res);
  }
  private static final String INTER_MNET = "INTER_MNET";
  private static final String INTER_MELON = "INTER_MELON";
  private static final String INTER_GAON = "INTER_GAON";
  private static final String INTER_MNET_MELON = "INTER_MNET_MELON";
  private static final String FINAL_OUTPUT = "FINAL_OUTPUT";
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

    if (otherArgs.length != 3) {
      System.err.println("Usage: MainDriver <Mnet> <Melon> <Gaon> ");
      System.exit(2);
    }
//-------------------------------------------------------------------------------------------------------------------------------
//  
//		Pre Job Operating Part1 - Mnet information refactory and merge
//
// ------------------------------------------------------------------------------------------------------------------------------
    
    Job job1 = new Job(getConf(), "MainDriver");

   FileOutputFormat.setOutputPath(job1, new Path(INTER_MNET));

    job1.setJarByClass(MainDriver.class);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);

    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

   job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    
   MultipleInputs.addInputPath(job1, new Path(otherArgs[0]),
    	      TextInputFormat.class, MnetMapper.class);
    job1.waitForCompletion(true);
//-------------------------------------------------------------------------------------------------------------------------------
//  
//		Pre Job Operating Part2 - Melon information refactory and merge
//
// ------------------------------------------------------------------------------------------------------------------------------

    
    Job job2 = new Job(getConf(), "MainDriver");

    FileOutputFormat.setOutputPath(job2, new Path(INTER_MELON));

    job2.setJarByClass(MainDriver.class);

    job2.setReducerClass(MelonReducer.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);

    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    
    MultipleInputs.addInputPath(job2, new Path(otherArgs[1]),
    	      TextInputFormat.class, MelonMapper.class);
    job2.waitForCompletion(true);
//-------------------------------------------------------------------------------------------------------------------------------
//  
//		Pre Job Operating Part3 - Gaon information refactory and merge
//
// ------------------------------------------------------------------------------------------------------------------------------
   
    Job job3 = new Job(getConf(), "MainDriver");

    FileOutputFormat.setOutputPath(job3, new Path(INTER_GAON));

    job3.setJarByClass(MainDriver.class);

    job3.setReducerClass(GaonReducer.class);

    job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(Text.class);

    job3.setInputFormatClass(TextInputFormat.class);
    job3.setOutputFormatClass(TextOutputFormat.class);

    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    
    MultipleInputs.addInputPath(job3, new Path(otherArgs[2]),
    	      TextInputFormat.class, GaonMapper.class);
    job3.waitForCompletion(true);
    
    
//-------------------------------------------------------------------------------------------------------------------------------
//  
//	Job Operating - information merge
//
// ------------------------------------------------------------------------------------------------------------------------------

    Job job4 = new Job(getConf(), "MainDriver");

    FileOutputFormat.setOutputPath(job4, new Path(INTER_MNET_MELON));

    job4.setJarByClass(MainDriver.class);
    job4.setPartitionerClass(CategoryCodeTaggedGroupKeyPartitioner.class);
    job4.setGroupingComparatorClass(CategoryCodeTaggedKeyComparator.class);
    job4.setSortComparatorClass(CategoryCodeTaggedKeyComparator.class);

    job4.setReducerClass(MnetMelonReducer.class);

    job4.setMapOutputKeyClass(CategoryCodeTaggedKey.class);
    job4.setMapOutputValueClass(Text.class);
    
    job4.setInputFormatClass(TextInputFormat.class);
    job4.setOutputFormatClass(TextOutputFormat.class);

    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(Text.class);
    
   MultipleInputs.addInputPath(job4, new Path(INTER_MELON),
  	      TextInputFormat.class, MelonMapper2.class);
   MultipleInputs.addInputPath(job4, new Path(INTER_MNET),
    	    	TextInputFormat.class, MnetMapper2.class);
     
    
    
    return job4.waitForCompletion(true) ? 0:1;
	
  }
}
