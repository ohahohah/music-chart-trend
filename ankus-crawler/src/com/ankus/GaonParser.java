package com.song;


import org.apache.hadoop.io.Text;

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
	      String[] colums = text.toString().split("ㅒ");

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
