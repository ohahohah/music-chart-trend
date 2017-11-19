package com.ankus;

import java.util.HashMap;

import org.apache.hadoop.io.Text;


/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : InterParser.java
* 3. 작성일 : 2017. 11. 20. 오전 1:25:53
* 4. 작성자 : mypc
* 5. 설명 : 중간 파싱
* </pre>
*/
public class InterParser {
	private String album;
	private String singer;
	private String sell;


  public InterParser(Text text) {
    try {
      String[] colums = text.toString().split("#");

          if (colums != null && colums.length > 0) {
        	  
    					album = colums[1].trim();
    					singer = colums[2].trim();
    					sell = colums[3].trim();

    				 
      }
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }
  public String getsell() {
		return sell;
	}

	public String getsinger() {
		return singer;
	}

	public String getalbum() {
		return album;
	}
  
  
}
