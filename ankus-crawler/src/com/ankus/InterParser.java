package com.song;

import java.util.HashMap;

import org.apache.hadoop.io.Text;

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
