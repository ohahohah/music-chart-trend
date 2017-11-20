package com.ankus;

import java.util.HashMap;

import org.apache.hadoop.io.Text;


/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : MelonParser.java
* 3. 작성일 : 2017. 11. 20. 오전 1:26:16
* 4. 작성자 : mypc
* 5. 설명 : 멜론 사이트 파싱
* </pre>
*/
public class MelonParser {
	private String ganre;
	private String song;
	private String singer;
	private String album;


  public MelonParser(Text text) {
    try {
      String[] colums = text.toString().split("��");

          if (colums != null && colums.length > 0) {
        	  
    					ganre = colums[0].trim();
    					song = colums[1].toUpperCase().replaceAll("!\"#[$]%&\\{\\}@`[*]:[+];-.<>,\\^~|'\\[\\]", "").trim()+" ";
    					if(song.contains("(")&&song.contains(")")){
    						song = song.substring(0, song.lastIndexOf("("))+song.substring(song.lastIndexOf(")")+1,song.length()-1).trim()+" ";
    						}
    					if(song.contains("(")&&song.contains(")")){
    						song = song.substring(0, song.lastIndexOf("("))+song.substring(song.lastIndexOf(")")+1,song.length()-1).trim()+" ";
    						}
    					singer = colums[2].toUpperCase().replaceAll("!\"#[$]%&\\{\\}@`[*]:[+];-.<>,\\^~|'\\[\\]", "").trim()+" ";
    					if(singer.contains("(")&&singer.contains(")")){
    						singer = singer.substring(0, singer.lastIndexOf("("))+singer.substring(singer.lastIndexOf(")")+1,singer.length()-1).trim()+" ";
    						}
    					if(singer.contains("(")&&singer.contains(")")){
    						singer = singer.substring(0, singer.lastIndexOf("("))+singer.substring(singer.lastIndexOf(")")+1,singer.length()-1).trim()+" ";
    						}
    					album = colums[3].toUpperCase().replaceAll("!\"#[$]%&\\{\\}@`[*]:[+];-.<>,\\^~|'\\[\\]", "").trim()+" ";
    					if(album.contains("(")&&album.contains(")")){
    						album = album.substring(0, album.lastIndexOf("("))+album.substring(album.lastIndexOf(")")+1,album.length()-1).trim()+" ";
    						}
    					if(album.contains("(")&&album.contains(")")){
    						album = album.substring(0, album.lastIndexOf("("))+album.substring(album.lastIndexOf(")")+1,album.length()-1).trim()+" ";
    						}
    				 
      }
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }
  	public MelonParser(Text text,int i) {
	    try {
	      String[] colums = text.toString().split("��");

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
