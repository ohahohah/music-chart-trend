package com.song;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GaonMapper extends
  Mapper<LongWritable, Text, Text, Text> {

  // map 출력값
  // map 출력키
  private Text outputKey = new Text();
  private Text outputValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    GaonParser parser = new GaonParser(value);

    // 출력키 설정
    if(parser.getalbum()!=null){
    	outputKey.set(parser.getalbum()+"ㅒ"+parser.getsinger());
    	outputValue.set(parser.getsell());
      context.write(outputKey, outputValue);
    	}
  }
}
