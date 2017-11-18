
package com.song;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class GaonReducer extends Reducer<Text, Text, Text, Text> {
  // 출력키
  private Text outputKey = new Text();
  // 출력값
  private Text outputValue = new Text();
  
  private int temp;
  public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

    Iterator<Text> iterator = values.iterator();
    outputKey.set(key+"ㅒ");
    Text info = new Text(iterator.next());	// input[0] 
    temp = Integer.parseInt(info.toString().trim());

    while (iterator.hasNext()) {
    	Text record = iterator.next();							// input[1]
    	if(temp<Integer.parseInt(record.toString().trim())){
    			temp = Integer.parseInt(record.toString().trim());
    		}
    	}
    outputValue.set(String.format("%d", temp));
    context.write(outputKey, outputValue);
    temp = 0;
  }  
  }
  
  
  




