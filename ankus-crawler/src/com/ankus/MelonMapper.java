package com.ankus;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : MelonMapper.java
* 3. 작성일 : 2017. 11. 20. 오전 1:26:07
* 4. 작성자 : mypc
* 5. 설명 : 멜론 사이트 매퍼
* </pre>
*/
public class MelonMapper extends
  Mapper<LongWritable, Text, Text, Text> {

  private Text outputKey = new Text();
  private Text outputValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

	  MelonParser parser = new MelonParser(value);

	  if((parser.getsong()!=null)&&(parser.getsinger()!=null)&&(parser.getalbum()!=null)&&(parser.getganre()!=null)){
		  outputKey.set(parser.getsong()+"��"+parser.getsinger()+"��"+parser.getalbum());
		  outputValue.set(parser.getganre());
		  context.write(outputKey, outputValue);
	  }
  }
}
