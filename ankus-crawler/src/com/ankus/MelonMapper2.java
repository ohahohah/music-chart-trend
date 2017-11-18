
package com.song;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MelonMapper2 extends Mapper<LongWritable, Text, CategoryCodeTaggedKey, Text> {
	CategoryCodeTaggedKey outputKey = new CategoryCodeTaggedKey();
  Text outValue = new Text();

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    MelonParser parser = new MelonParser(value,0);

    outputKey.setTag(1);
    outputKey.setcategorycode(parser.getsong()+"ㅒ"+parser.getsinger()+"ㅒ"+parser.getalbum());
    outValue.set(parser.getganre());
    context.write(outputKey, outValue);
  }
}

