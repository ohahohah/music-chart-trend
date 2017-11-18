package com.song;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CategoryCodeTaggedGroupKeyPartitioner extends Partitioner<CategoryCodeTaggedKey, Text> {

  @Override
  public int getPartition(CategoryCodeTaggedKey key, Text val, int numPartitions) {
   
    int hash = key.getcategorycode().hashCode();
    int partition = hash % numPartitions;
    return partition;
  }
}