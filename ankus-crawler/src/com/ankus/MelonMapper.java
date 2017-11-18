package com.song;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MelonMapper extends
  Mapper<LongWritable, Text, Text, Text> {

  // map 출력값
  // map 출력키
  private Text outputKey = new Text();
  private Text outputValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    MelonParser parser = new MelonParser(value);

    // 출력키 설정
    if(parser.getsong()!=null){
    	outputKey.set(parser.getsong()+"ㅒ"+parser.getsinger()+"ㅒ"+parser.getalbum());
    	outputValue.set(parser.getganre());
      context.write(outputKey, outputValue);
    	}
  }
}
