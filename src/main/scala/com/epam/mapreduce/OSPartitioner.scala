package com.epam.mapreduce

import com.epam.mapreduce.types.Record
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Partitioner

/*
 * Use info about Operation System for partition amound reducers
 */
class OSPartitioner extends Partitioner[Text, Record]{
  override def getPartition(key: Text, value: Record, i: Int): Int = {
    value.os.hashCode % i
  }
}
