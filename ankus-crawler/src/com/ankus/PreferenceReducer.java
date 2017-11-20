
package com.ankus;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : PreferenceReducer.java
* 3. 작성일 : 2017. 11. 20. 오전 1:27:13
* 4. 작성자 : mypc
* 5. 설명 : 선호도 계산 리듀서
* </pre>
*/
public class PreferenceReducer extends Reducer<Text, Text, Text, Text> {
  private Text outputKey = new Text();
  private Text outputValue = new Text();
  
  private long temp=0;
  public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

    Iterator<Text> iterator = values.iterator();
    outputKey.set(key);
    
    while (iterator.hasNext()) {
    	Text record = iterator.next();
    	temp += Integer.parseInt(record.toString().trim());
    }
    outputValue.set(String.format("%d", temp));
    context.write(outputKey, outputValue);
    temp = 0;
  }  
}
  
  
  




