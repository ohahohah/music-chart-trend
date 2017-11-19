
package com.ankus;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : GaonReducer.java
* 3. 작성일 : 2017. 11. 20. 오전 1:13:15
* 4. 작성자 : mypc
* 5. 설명 : 가온 차트 리듀서
* </pre>
*/
public class GaonReducer extends Reducer<Text, Text, Text, Text> {
  private Text outputKey = new Text();
  private Text outputValue = new Text();
  
  private int temp;
  public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

    Iterator<Text> iterator = values.iterator();
    outputKey.set(key+"�뀙");
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
  
  
  




