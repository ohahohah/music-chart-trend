
package com.ankus;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : PreferenceReducer2.java
* 3. 작성일 : 2017. 11. 20. 오전 1:27:17
* 4. 작성자 : mypc
* 5. 설명 : 선호도 계산 2차 리듀서
* </pre>
*/
public class PreferenceReducer2 extends Reducer<Text, Text, Text, Text> {
  private Text outputKey = new Text();
  private Text outputValue = new Text();
  private String[] ganre = new String[8];
  private float[] array = new float[8];
  private float sum=0;
  private int count=0;
  public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
	    Iterator<Text> iterator = values.iterator();
	    while (iterator.hasNext()) {
	    	Text record = iterator.next();
	    	ganre[count]=record.toString().split("#")[0];
	    	array[count]=Integer.parseInt(record.toString().split("#")[1]);
	    	count++;
    	}
    
	    for(int i=0;i<count;i++){
	    	sum += array[i];
    	}
    
	    for(int j=0;j<count;j++){
	    	outputKey.set(key+","+ganre[j]+",");
	    	outputValue.set(String.format("%.2f",array[j]/sum));
	    	context.write(outputKey, outputValue);
    	}
	    sum=0;
	    count=0;
  	}  
 }
  
  
  




