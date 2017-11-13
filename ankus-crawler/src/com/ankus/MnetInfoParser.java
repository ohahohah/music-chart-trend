package com.ankus;


import org.apache.hadoop.io.Text;

public class MnetInfoParser {
	private String category;
	private String categorycode;
	private String size;
	private String sex;

  public MnetInfoParser(Text text) {
    try {
      String[] colums = text.toString().trim().split("#");
      if (colums != null && colums.length > 0) {
      
      categorycode = colums[1];
      
      
    	  if (colums[3].equalsIgnoreCase("1")){
    		  size = "7";
    	  }else if(colums[3].equalsIgnoreCase("2")){
    		  size = "20";
    	  }
      
      sex = colums[4];
      
      }
    } catch (Exception e) {
      System.out.println("Error parsing a record :" + e.getMessage());
    }
  }
  public String getcategorycode(){return categorycode;}
  public String getcategory() { return category; }
  public String getsex() { return sex;}
  public String getsize() { return size;}
  
  
}
