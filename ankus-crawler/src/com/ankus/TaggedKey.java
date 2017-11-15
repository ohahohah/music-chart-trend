package com.ankus;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaggedKey implements WritableComparable<TaggedKey> {
  private String Code;
  private Integer tag;

  public TaggedKey() {}

  public TaggedKey(String Code, int tag) {
    this.Code = Code;
    this.tag = tag;
  }

  public String getCode() {
    return Code;
  }

  public Integer getTag() {
    return tag;
  }

  public void setCode(String Code) {
    this.Code = Code;
  }

  public void setTag(Integer tag) {
    this.tag = tag;
  }

  public int compareTo(TaggedKey key) {
    int result = this.Code.compareTo(key.Code);

    if (result == 0) {
      return  this.tag.compareTo(key.tag);
    }

    return result;
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, Code);
    out.writeInt(tag);
  }

  public void readFields(DataInput in) throws IOException {
	  Code = WritableUtils.readString(in);
    tag = in.readInt();
  }
}
