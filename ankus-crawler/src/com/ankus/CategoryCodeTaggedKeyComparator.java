package com.ankus;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : CategoryCodeTaggedKeyComparator.java
* 3. 작성일 : 2017. 11. 20. 오전 1:25:16
* 4. 작성자 : mypc
* 5. 설명 : 복합키 비교기
* </pre>
*/
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
