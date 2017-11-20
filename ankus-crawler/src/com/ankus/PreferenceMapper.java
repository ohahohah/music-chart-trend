package com.ankus;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : PreferenceMapper.java
* 3. 작성일 : 2017. 11. 20. 오전 1:27:01
* 4. 작성자 : mypc
* 5. 설명 : 선호도 계산 매퍼
* </pre>
*/
public class PreferenceMapper extends
  Mapper<LongWritable, Text, Text, Text> {

  private Text outputKey = new Text();
  private Text outputValue = new Text();
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    PreferenceParser parser = new PreferenceParser(value);

    if(parser.getdate()!=null&&parser.getprefer().length()!=0){
    	outputKey.set(parser.getdate()+"#"+parser.getganre()+"#");
    	outputValue.set(parser.getprefer());
      context.write(outputKey, outputValue);
    	}
  }
}
