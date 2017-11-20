
package com.ankus;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : MnetReducer.java
* 3. 작성일 : 2017. 11. 20. 오전 1:26:50
* 4. 작성자 : mypc
* 5. 설명 : 엠넷 차트 리듀서
* </pre>
*/
public class MnetReducer extends Reducer<Text, Text, Text, Text> {
  private Text outputKey = new Text();
  private Text outputValue = new Text();
  
  private String temp="";
  public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

    Iterator<Text> iterator = values.iterator();
    outputKey.set(key+"��");

    while (iterator.hasNext()) {
    	Text record = iterator.next();
    	temp += record.toString().trim();
    	}
    outputValue.set(temp);
    context.write(outputKey, outputValue);
    
  	}  
  }
  
  
  




