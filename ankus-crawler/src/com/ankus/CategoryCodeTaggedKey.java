package com.song;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CategoryCodeTaggedKey implements WritableComparable<CategoryCodeTaggedKey> {
  // 항공사 코드
  private String categorycode;
  // 조인 테그
  private Integer tag;
  //private String major;

  public CategoryCodeTaggedKey() {}

  public CategoryCodeTaggedKey(String categorycode, int tag) {
    this.categorycode = categorycode;
    this.tag = tag;
    //this.major = major;
  }

  public String getcategorycode() {
    return categorycode;
  }

  public Integer getTag() {
    return tag;
  }
 // public String getmajor(){
//	  return major;
 // }
  public void setcategorycode(String categorycode) {
    this.categorycode = categorycode;
  }

  public void setTag(Integer tag) {
    this.tag = tag;
  }
 // public void setmajor(String major){
	//  this.major = major;
  //}
  public int compareTo(CategoryCodeTaggedKey key) {
    int result = this.categorycode.compareTo(key.categorycode);

    if (result == 0) {
      return  this.tag.compareTo(key.tag);
    }

    return result;
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, categorycode);
    out.writeInt(tag);
   // WritableUtils.writeString(out, major);
  }

  public void readFields(DataInput in) throws IOException {
	  categorycode = WritableUtils.readString(in);
    tag = in.readInt();
    //major = WritableUtils.readString(in);
  }
}
