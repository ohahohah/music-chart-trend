package com.song;


import org.apache.hadoop.io.Text;

public class MnetParser {
	private String rank;
	private String date;
	private String song;
	private String singer;
	private String album;


  public MnetParser(Text text) {
    try {
      String[] colums = text.toString().split("ㅒ");

          if (colums != null && colums.length > 0) {
        	  
    					rank = colums[0].trim();
    					date = colums[1].trim();
    					song = colums[2].trim();
    					singer = colums[3].trim();
    					album = colums[4].trim();

    				 
      }
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }
  public MnetParser(Text text,int i) {
	    try {
	      String[] colums = text.toString().split("ㅒ");

	          if (colums != null && colums.length > 0) {
	        	  
	    					rank = colums[4].trim();
	    					date = colums[3].trim();
	    					song = colums[0].trim();
	    					singer = colums[1].trim();
	    					album = colums[2].trim();

	    				 
	      }
	    } catch (Exception e) {
	      System.out.println("Error parsing a record :" + e.getMessage());
	    }
	  }
  public String getrank() {
		return rank;
	}

	public String getdate() {
		return date;
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
