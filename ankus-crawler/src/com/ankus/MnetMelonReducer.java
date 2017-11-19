package com.ankus;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : MnetMelonReducer.java
* 3. 작성일 : 2017. 11. 20. 오전 1:26:40
* 4. 작성자 : mypc
* 5. 설명 : 엠넷 멜론 크롤링 데이터 리듀서
* </pre>
*/
public class MnetMelonReducer extends Reducer<CategoryCodeTaggedKey, Text, Text, Text> {
  private Text outputKey = new Text();
  private Text outputValue = new Text();
  private String ganre;
  private String rank;
  public void reduce(CategoryCodeTaggedKey key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

    Iterator<Text> iterator = values.iterator();
    Text info = new Text(iterator.next());
    ganre = info.toString();
    System.out.println("key:"+key.getcategorycode());
    System.out.println("ganre:"+ganre);
    if(!ganre.contains("2")){
    	outputKey.set("key:"+key.getcategorycode()+"&"+ganre+"&");
    	while(iterator.hasNext()){
    		Text record = new Text(iterator.next());
    		rank = record.toString();
    		System.out.println("rank:"+rank);
    		outputValue.set("+value:"+rank);
    		context.write(outputKey, outputValue);
		}
    }
  }
}



