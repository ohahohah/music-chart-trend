package com.song;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CategoryCodeTaggedKeyComparator extends WritableComparator {
  protected CategoryCodeTaggedKeyComparator() {
    super(CategoryCodeTaggedKey.class, true);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    CategoryCodeTaggedKey k1 = (CategoryCodeTaggedKey) w1;
    CategoryCodeTaggedKey k2 = (CategoryCodeTaggedKey) w2;

    int cmp = k1.getcategorycode().compareTo(k2.getcategorycode());
    if (cmp != 0) {
      return cmp;
    }

    return k1.getTag().compareTo(k2.getTag());
  }
}
