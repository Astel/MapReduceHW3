package com.epam.mapreduce

import com.epam.mapreduce.types.Record
import eu.bitwalker.useragentutils.UserAgent
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.io.Source

/*
 * Mapper from text format to key = cityname, value = (operating system, count = 1)
 * write only if price more than 250
 * os used for partitioner
 */
class Map extends Mapper[LongWritable, Text, Text, Record] {
  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, Record]#Context): Unit = {
    val cities = context.getCacheFiles.find(uri =>
      FilenameUtils.getName(uri.getPath).equals("city_en.txt")) match {
      case Some(f) => f
      case None    => throw new IllegalStateException("Missing cache")
    }
    for (log <- value.toString.split("\n")) {
      val list = log.split("\t")
      val id = list(7)

      val os = UserAgent.parseUserAgentString(list(4)).getOperatingSystem.getGroup.getName

      //
      val cityName = Source
        .fromURI(cities)
        .getLines()
        .find(str => str.contains(id))
        .getOrElse("unknown")
        .split("\t")
        .tail
        .head //second value

      val price = list(19).toInt
      if (price > 250) context.write(new Text(cityName), new Record(os = os, count = 1))
    }
  }
}
