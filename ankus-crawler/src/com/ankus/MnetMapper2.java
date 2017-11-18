package com.song;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MnetMapper2 extends Mapper<LongWritable, Text, CategoryCodeTaggedKey, Text> {
	CategoryCodeTaggedKey outputKey = new CategoryCodeTaggedKey();
  Text outValue = new Text();

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {


    outputKey.setTag(0);
    MnetParser parser = new MnetParser(value,0);
    outputKey.setcategorycode(parser.getsong()+"ㅒ"+parser.getsinger()+"ㅒ"+parser.getalbum());
    outValue.set(parser.getdate()+"ㅒ"+parser.getrank());
    context.write(outputKey, outValue);
    	
  }
}

