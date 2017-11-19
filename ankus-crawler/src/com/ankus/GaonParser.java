package com.ankus;


import org.apache.hadoop.io.Text;

/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : GaonParser.java
* 3. 작성일 : 2017. 11. 20. 오전 1:13:04
* 4. 작성자 : mypc
* 5. 설명 : 가온 차트 파싱
* </pre>
*/

public class GaonParser {
	private String album;
	private String singer;
	private String[] sell;


  public GaonParser(Text text) {
    try {
      String[] colums = text.toString().split("ㅒ");

          if (colums != null && colums.length > 0) {
        	  
    					album = colums[1];
    					singer = colums[2];
    					sell = colums[3].split("/");
    					if(sell[0].contains(",")){
    						sell[0] = sell[0].substring(0, sell[0].indexOf(","))+sell[0].substring(sell[0].indexOf(",")+1,sell[0].length()-1);
    						}
    					if(sell[0].contains(",")){
    						sell[0] = sell[0].substring(0, sell[0].indexOf(","))+sell[0].substring(sell[0].indexOf(",")+1,sell[0].length()-1);
    						}
      }
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }
  public GaonParser(Text text,int i) {
	    try {
	      String[] colums = text.toString().split("�뀙");

	          if (colums != null && colums.length > 0) {
	        	  
	    					album = colums[0];
	    					singer = colums[1];
	    					sell[0] = colums[2];
	      }
	    } catch (Exception e) {
	      System.out.println("Error parsing a record :" + e.getMessage());
	    }
	  }
  	public String getsell() {
		return sell[0];
	}

	public String getsinger() {
		return singer;
	}

	public String getalbum() {
		return album;
	}
  
  
}
