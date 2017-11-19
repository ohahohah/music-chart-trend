package com.ankus;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : CategoryCodeTaggedGroupKeyPartitioner.java
* 3. 작성일 : 2017. 11. 20. 오전 1:25:05
* 4. 작성자 : mypc
* 5. 설명 : 복합키 파티셔너
* </pre>
*/
public class CategoryCodeTaggedGroupKeyPartitioner extends Partitioner<CategoryCodeTaggedKey, Text> {

  @Override
  public int getPartition(CategoryCodeTaggedKey key, Text val, int numPartitions) {
   
    int hash = key.getcategorycode().hashCode();
    int partition = hash % numPartitions;
    return partition;
  }
}