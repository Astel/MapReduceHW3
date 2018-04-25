package com.epam.mapreduce

import com.epam.mapreduce.types.Record
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Counter
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar

class MapTest extends FlatSpec with MockitoSugar {
  val one = new IntWritable(1)

  it should "check write cityname, size and os to context" in {
    val mapper = new Map
    val context = mock[mapper.Context]
    when(context.getCacheFiles).thenReturn(Array(getClass.getResource("/city_en.txt").toURI))

    mapper.map(
      key = null,
      value = new Text("a76f978b4d6284b0bbd9cf5372659a19\t20131019053006837\t2\tDAJ5U6D5v7r\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; 2345Explorer)\t112.90.194.*\t216\t232\t3\tdd4270481b753dde29898e27c7c03920\tbc253240f1335129b1314e5b8a83d276\tnull\tEnt_Pic_Width2\t1000\t90\tNa\tNa\t10\t7336\t294\t160\tnull\t2259\tnull"),
      context
    )

    verify(context).write(ArgumentMatchers.any[Text], ArgumentMatchers.any[Record])
  }
}