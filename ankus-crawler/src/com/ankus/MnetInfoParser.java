package com.ankus;


import org.apache.hadoop.io.Text;

public class MnetInfoParser {
	private String rank;
	private String date;
	private String song;
	private String singer;
	private String album;
	//private String ganre;
	//private String sell;

  public MnetInfoParser(Text text) {
    try {
      String[] colums = text.toString().trim().split("#");
      if (colums != null && colums.length > 0) {
    	  rank = colums[0];
    	  date = colums[1];
    	  song = colums[2];
    	  singer = colums[3];
    	  album = colums[4];
      }
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }
  public String getrank(){return rank;}
  public String getdate() { return date; }
  public String getsong() { return song;}
  public String getsinger() { return singer;}
  public String getalbum() { return album;}
  
}
