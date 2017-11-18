package com.song;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MnetMelonReducer extends Reducer<CategoryCodeTaggedKey, Text, Text, Text> {
  // 출력키
  private Text outputKey = new Text();
  // 출력값
  private Text outputValue = new Text();
  private String[] ad;
  private String ganre;
  public void reduce(CategoryCodeTaggedKey key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

    Iterator<Text> iterator = values.iterator();
    outputKey.set("key:"+key.getcategorycode()+"ㅒ");
    Text info = new Text(iterator.next());	// input[0]
    ganre = info.toString();
    while(iterator.hasNext()){
    	Text record = new Text(iterator.next());
	    outputValue.set("\t\tvalue:"+ganre+record.toString());
	    context.write(outputKey, outputValue);
    	 	
    	 }
  
    
  }
}



