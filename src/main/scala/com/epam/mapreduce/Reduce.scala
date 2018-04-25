package com.epam.mapreduce

import java.lang

import com.epam.mapreduce.types.Record
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters._

class Reduce extends Reducer[Text, Record, Text, IntWritable] {
  override def reduce(key: Text, values: lang.Iterable[Record], context: Reducer[Text, Record, Text, IntWritable]#Context): Unit = {
    val sum = values.asScala.map(_.count).sum
    context.write(key, new IntWritable(sum))
  }
}
