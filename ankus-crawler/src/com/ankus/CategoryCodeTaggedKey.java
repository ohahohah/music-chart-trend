package com.ankus;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : CategoryCodeTaggedKey.java
* 3. 작성일 : 2017. 11. 20. 오전 1:25:12
* 4. 작성자 : mypc
* 5. 설명 : 복합키 구성
* </pre>
*/
public class CategoryCodeTaggedKey implements WritableComparable<CategoryCodeTaggedKey> {
  private String categorycode;
  private Integer tag;

  public CategoryCodeTaggedKey() {}

  public CategoryCodeTaggedKey(String categorycode, int tag) {
    this.categorycode = categorycode;
    this.tag = tag;
  }

  public String getcategorycode() {
    return categorycode;
  }

  public Integer getTag() {
    return tag;
  }
  public void setcategorycode(String categorycode) {
    this.categorycode = categorycode;
  }

  public void setTag(Integer tag) {
    this.tag = tag;
  }
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
  }

  public void readFields(DataInput in) throws IOException {
	  categorycode = WritableUtils.readString(in);
    tag = in.readInt();
  }
}
