package com.ankus;


import org.apache.hadoop.io.Text;


/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : PreferenceParser.java
* 3. 작성일 : 2017. 11. 20. 오전 1:27:09
* 4. 작성자 : mypc
* 5. 설명 : 선호도 계산 데이터 파싱
* </pre>
*/
public class PreferenceParser {
	private String date;
	private String ganre;
	private String prefer;


	public PreferenceParser(Text text) {
	  try {
		  String[] colums = text.toString().split("#");
		  if (colums != null && colums.length > 0) {
        	date = colums[1].trim();
    		ganre = colums[3].trim();
    		prefer = colums[4].trim();
          }
	  } catch (Exception e) {
		  System.out.println("Error parsing a record :" + e.getMessage());
    	}
	}
	public PreferenceParser(Text text,int i) {
	  try {
	      String[] colums = text.toString().split("#");
	      if (colums != null && colums.length > 0) {
	        date = colums[0].trim();
	    	ganre = colums[1].trim();
	    	prefer = colums[2].trim();
	      }
	  } catch (Exception e) {
	      System.out.println("Error parsing a record :" + e.getMessage());
	  	}
	}
 
  public String getdate() {
		return date;
  }

	public String getganre() {
		return ganre;
  }

	public String getprefer() {
		return prefer;
  }
  
  
}
