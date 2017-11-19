
package com.ankus;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : MelonMapper2.java
* 3. 작성일 : 2017. 11. 20. 오전 1:26:12
* 4. 작성자 : mypc
* 5. 설명 : 멜론 사이트 2차 매퍼
* </pre>
*/
public class MelonMapper2 extends Mapper<LongWritable, Text, CategoryCodeTaggedKey, Text> {
	CategoryCodeTaggedKey outputKey = new CategoryCodeTaggedKey();
  Text outValue = new Text();

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

	  MelonParser parser = new MelonParser(value,0);

	  outputKey.setTag(0);
	  outputKey.setcategorycode(parser.getsong()+"&"+parser.getsinger()+"&"+parser.getalbum());
	  outValue.set(parser.getganre());
	  context.write(outputKey, outValue);
  }
}

