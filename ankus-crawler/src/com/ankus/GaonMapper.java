package com.ankus;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : GaonMapper.java
* 3. 작성일 : 2017. 11. 20. 오전 1:25:24
* 4. 작성자 : mypc
* 5. 설명 : 가온 차트 매퍼
* </pre>
*/
public class GaonMapper extends
  Mapper<LongWritable, Text, Text, Text> {

  private Text outputKey = new Text();
  private Text outputValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    GaonParser parser = new GaonParser(value);

    if(parser.getalbum()!=null){
    	outputKey.set(parser.getalbum()+"��"+parser.getsinger());
    	outputValue.set(parser.getsell());
      context.write(outputKey, outputValue);
    	}
  }
}
