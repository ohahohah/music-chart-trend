package com.ankus;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MnetMapper extends
  Mapper<LongWritable, Text, Text, Text> {
	Text outValue = new Text();
	Text outputKey = new Text();
  
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    MnetInfoParser parser = new MnetInfoParser(value);
    outputKey.set(parser.getcategorycode());
    outValue.set(parser.getsize()+"#"+parser.getsex());
    context.write(outputKey, outValue);
    
  }
}
