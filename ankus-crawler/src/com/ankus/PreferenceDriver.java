package com.ankus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
* 2. 타입명 : PreferenceDriver.java
* 3. 작성일 : 2017. 11. 20. 오전 1:26:56
* 4. 작성자 : mypc
* 5. 설명 : 선호도 계산을 위한 메인 드라이버
* </pre>
*/
public class PreferenceDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PreferenceDriver(), args);
    System.out.println("MR-Job Result:" + res);
  }
  private static final String FINAL_PREFERENCE = "FINAL_PREFERENCE";
  private static final String PREFERENCE = "PREFERENCE";
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

    if (otherArgs.length != 1) {
      System.err.println("Usage: PreferenceDriver <last> ");
      System.exit(2);
    }
//-------------------------------------------------------------------------------------------------------------------------------
//  
//		Job Operating Part1 - Preference calculate
//
// ------------------------------------------------------------------------------------------------------------------------------
    
    Job job1 = new Job(getConf(), "PreferenceDriver");

    FileOutputFormat.setOutputPath(job1, new Path(PREFERENCE));

    job1.setJarByClass(PreferenceDriver.class);

    job1.setReducerClass(PreferenceReducer.class);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);

    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    
    MultipleInputs.addInputPath(job1, new Path(otherArgs[0]),
    	      TextInputFormat.class, PreferenceMapper.class);
    job1.waitForCompletion(true);
    
//-------------------------------------------------------------------------------------------------------------------------------
//  
//		Job Operating Part2 - Preference of ganre calculate
//
// ------------------------------------------------------------------------------------------------------------------------------
    Job job2 = new Job(getConf(), "PreferenceDriver");

    FileOutputFormat.setOutputPath(job2, new Path(FINAL_PREFERENCE));

    job2.setJarByClass(PreferenceDriver.class);

    job2.setReducerClass(PreferenceReducer2.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);

    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    
    MultipleInputs.addInputPath(job2, new Path(PREFERENCE),
    	      TextInputFormat.class, PreferenceMapper2.class);
    
    return job2.waitForCompletion(true) ? 0:1;
  }
}
