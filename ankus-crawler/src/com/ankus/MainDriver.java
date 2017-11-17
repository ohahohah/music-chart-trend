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



public class MainDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    // Tool �씤�꽣�럹�씠�뒪 �떎�뻾
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

    // �엯�젰異� �뜲�씠�꽣 寃쎈줈 �솗�씤
    if (otherArgs.length != 3) {
      System.err.println("Usage: MainDriver <Mnet> <Melon> <Gaon> ");
      System.exit(2);
    }
//-------------------------------------------------------------------------------------------------------------------------------
//  
//		Pre Job Operating Part1 - Mnet, Melon information refactory and merge
//
// ------------------------------------------------------------------------------------------------------------------------------
    
    Job job1 = new Job(getConf(), "MainDriver");

    // 異쒕젰 �뜲�씠�꽣 寃쎈줈 �꽕�젙
    FileOutputFormat.setOutputPath(job1, new Path(INTER_MNET));

    // Job �겢�옒�뒪 �꽕�젙
    job1.setJarByClass(MainDriver.class);

    // Reducer �겢�옒�뒪 �꽕�젙
    //job1.setReducerClass(MnetReducer.class);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);

    // �엯異쒕젰 �뜲�씠�꽣 �룷留� �꽕�젙
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    // 異쒕젰�궎 諛� 異쒕젰媛� �쑀�삎 �꽕�젙
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    
    // MultipleInputs �꽕�젙
    MultipleInputs.addInputPath(job1, new Path(otherArgs[0]),
    	      TextInputFormat.class, MnetMapper.class);
    job1.waitForCompletion(true);
    
    
 //---------------------------------------------------------------------------------------------------------------------------------   
    
    
    Job job2 = new Job(getConf(), "MainDriver");

    // 異쒕젰 �뜲�씠�꽣 寃쎈줈 �꽕�젙
    FileOutputFormat.setOutputPath(job2, new Path(INTER_MELON));

    // Job �겢�옒�뒪 �꽕�젙
    job2.setJarByClass(MainDriver.class);

    // Reducer �겢�옒�뒪 �꽕�젙
    job2.setReducerClass(MelonReducer.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);

    // �엯異쒕젰 �뜲�씠�꽣 �룷留� �꽕�젙
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    // 異쒕젰�궎 諛� 異쒕젰媛� �쑀�삎 �꽕�젙
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    
    // MultipleInputs �꽕�젙
    MultipleInputs.addInputPath(job2, new Path(otherArgs[1]),
    	      TextInputFormat.class, MelonMapper.class);
    job2.waitForCompletion(true);
    //return job2.waitForCompletion(true) ? 0:1;
 //--------------------------------------------------------------------------------------------------------------------------   
    
    Job job3 = new Job(getConf(), "MainDriver");

    // 異쒕젰 �뜲�씠�꽣 寃쎈줈 �꽕�젙
    FileOutputFormat.setOutputPath(job3, new Path(INTER_GAON));

    // Job �겢�옒�뒪 �꽕�젙
    job3.setJarByClass(MainDriver.class);

    // Reducer �겢�옒�뒪 �꽕�젙
    job3.setReducerClass(GaonReducer.class);

    job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(Text.class);

    // �엯異쒕젰 �뜲�씠�꽣 �룷留� �꽕�젙
    job3.setInputFormatClass(TextInputFormat.class);
    job3.setOutputFormatClass(TextOutputFormat.class);

    // 異쒕젰�궎 諛� 異쒕젰媛� �쑀�삎 �꽕�젙
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    
    // MultipleInputs �꽕�젙
    MultipleInputs.addInputPath(job3, new Path(otherArgs[2]),
    	      TextInputFormat.class, GaonMapper.class);
    job3.waitForCompletion(true);
    //return job3.waitForCompletion(true) ? 0:1;
    
    
//-------------------------------------------------------------------------------------------------------------------------------
    // Job �씠由� �꽕�젙
    Job job4 = new Job(getConf(), "MainDriver");

    // 異쒕젰 �뜲�씠�꽣 寃쎈줈 �꽕�젙
    FileOutputFormat.setOutputPath(job4, new Path(INTER_MNET_MELON));

    // Job �겢�옒�뒪 �꽕�젙
    job4.setJarByClass(MainDriver.class);
    job4.setPartitionerClass(CategoryCodeTaggedGroupKeyPartitioner.class);
    job4.setGroupingComparatorClass(CategoryCodeTaggedKeyComparator.class);
    job4.setSortComparatorClass(CategoryCodeTaggedKeyComparator.class);

    // Reducer �겢�옒�뒪 �꽕�젙
    job4.setReducerClass(MnetMelonReducer.class);

    job4.setMapOutputKeyClass(CategoryCodeTaggedKey.class);
    job4.setMapOutputValueClass(Text.class);
    
    // �엯異쒕젰 �뜲�씠�꽣 �룷留� �꽕�젙
    job4.setInputFormatClass(TextInputFormat.class);
    job4.setOutputFormatClass(TextOutputFormat.class);

    // 異쒕젰�궎 諛� 異쒕젰媛� �쑀�삎 �꽕�젙
    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(Text.class);
    
    // MultipleInputs �꽕�젙
   MultipleInputs.addInputPath(job4, new Path(INTER_MELON),
  	      TextInputFormat.class, MelonMapper2.class);
   MultipleInputs.addInputPath(job4, new Path(INTER_MNET),
    	    	TextInputFormat.class, MnetMapper2.class);
     
    
    
    return job4.waitForCompletion(true) ? 0:1;
    
//-------------------------------------------------------------------------------------------------------------------------------
//  
//		Pre Job Operating Part2 - funding information refactorying
//
//-------------------------------------------------------------------------------------------------------------------------------
    // Job �씠由� �꽕�젙
    /*Job job5 = new Job(getConf(), "MainDriver");

    // 異쒕젰 �뜲�씠�꽣 寃쎈줈 �꽕�젙
    FileOutputFormat.setOutputPath(job5, new Path(FINAL_OUTPUT));

    // Job �겢�옒�뒪 �꽕�젙
    job5.setJarByClass(MainDriver.class);
    job5.setPartitionerClass(CategoryCodeTaggedGroupKeyPartitioner.class);
    job5.setGroupingComparatorClass(TaggedGroupKeyComparator.class);
    job5.setSortComparatorClass(CategoryCodeTaggedKeyComparator.class);

    // Reducer �겢�옒�뒪 �꽕�젙
    //job5.setReducerClass(WeekToSocietyReducer.class);

    job5.setMapOutputKeyClass(CategoryCodeTaggedKey.class);
    job5.setMapOutputValueClass(Text.class);
    
    // �엯異쒕젰 �뜲�씠�꽣 �룷留� �꽕�젙
    job5.setInputFormatClass(TextInputFormat.class);
    job5.setOutputFormatClass(TextOutputFormat.class);

    // 異쒕젰�궎 諛� 異쒕젰媛� �쑀�삎 �꽕�젙
    job5.setOutputKeyClass(Text.class);
    job5.setOutputValueClass(Text.class);
    
    // MultipleInputs �꽕�젙
    MultipleInputs.addInputPath(job5, new Path(INTER_MNET),
	  	      TextInputFormat.class, MnetMapper.class);
	  MultipleInputs.addInputPath(job5, new Path(INTER_MELON_GAON),
	  	      TextInputFormat.class, InterMapper.class);
    return job5.waitForCompletion(true) ? 0:1;*/
	
  }
}
