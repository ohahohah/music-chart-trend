package com.ankus;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MnetMapper extends
  Mapper<LongWritable, Text, Text, Text> {

  // map 異쒕젰媛�
  // map 異쒕젰�궎
  private Text outputKey = new Text();
  private Text outputValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    MnetParser parser = new MnetParser(value);

    // 異쒕젰�궎 �꽕�젙
    if(parser.getsong()!=null){
    	outputKey.set(parser.getsong()+"�뀙"+parser.getsinger()+"�뀙"+parser.getalbum()+"�뀙");
    	outputValue.set(parser.getdate()+"�뀙"+parser.getrank());
      context.write(outputKey, outputValue);
    	}
  }
}
