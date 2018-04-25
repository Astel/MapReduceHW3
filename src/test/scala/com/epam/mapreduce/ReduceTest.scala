package com.epam.mapreduce

import com.epam.mapreduce.types.Record
import org.apache.hadoop.io.{IntWritable, Text}
import org.mockito.Mockito.verify
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class ReduceTest extends FlatSpec with MockitoSugar {
  it should "output cityname and total count" in {
    val reducer = new Reduce
    val context = mock[reducer.Context]

    reducer.reduce(
      key = new Text("cityname"),
      values = Seq(new Record(10000, "Win"), new Record(10000, "Win"), new Record(0, "Win")).asJava,
      context
    )

    verify(context).write(new Text("cityname"), new IntWritable(20000))
  }
}

