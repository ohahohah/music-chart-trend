package com.song;

import java.util.HashMap;

import org.apache.hadoop.io.Text;

public class MelonParser {
	private String ganre;
	private String song;
	private String singer;
	private String album;


  public MelonParser(Text text) {
    try {
      String[] colums = text.toString().split("ㅒ");

          if (colums != null && colums.length > 0) {
        	  
    					ganre = colums[0].trim();
    					song = colums[1].trim();
    					singer = colums[2].trim();
    					album = colums[3].trim();

    				 
      }
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }
  public MelonParser(Text text,int i) {
	    try {
	      String[] colums = text.toString().split("ㅒ");

	          if (colums != null && colums.length > 0) {
	        	  
	    					song = colums[0].trim();
	    					singer = colums[1].trim();
	    					album = colums[2].trim();
	    					ganre = colums[3].trim();
	    				 
	      }
	    } catch (Exception e) {
	      System.out.println("Error parsing a record :" + e.getMessage());
	    }
	  }
  public String getganre() {
		return ganre;
	}

	public String getsong() {
		return song;
	}

	public String getsinger() {
		return singer;
	}

	public String getalbum() {
		return album;
	}
  
  
}
