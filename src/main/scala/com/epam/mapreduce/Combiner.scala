package com.epam.mapreduce

import java.lang

import com.epam.mapreduce.types.Record
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters._

class Combiner extends Reducer[Text, Record, Text, Record] {
  override def reduce(key: Text, values: lang.Iterable[Record], context: Reducer[Text, Record, Text, Record]#Context): Unit = {
    val sum = values.asScala.foldLeft(new Record())((l, r) => {
      new Record(l.count + r.count, if (l.os.equals("none")) r.os else l.os)
    })
    context.write(key, sum)
  }
}
