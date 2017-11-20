package com.ankus;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : InterMapper.java
* 3. 작성일 : 2017. 11. 20. 오전 1:25:48
* 4. 작성자 : mypc
* 5. 설명 : 중간 매퍼
* </pre>
*/
public class InterMapper extends
  Mapper<LongWritable, Text, Text, Text> {

 private Text outputKey = new Text();
  private Text outputValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    InterParser parser = new InterParser(value);

    if(parser.getalbum()!=null){
    	outputKey.set(parser.getalbum());
      context.write(outputKey, outputValue);
    }
  }
}
