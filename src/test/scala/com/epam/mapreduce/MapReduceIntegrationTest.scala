package com.epam.mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.{IntWritable, SequenceFile, Text}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

class MapReduceIntegrationTest extends FlatSpec with Matchers  with BeforeAndAfterAll with BeforeAndAfterEach{
  it should "write out word counts to output folder" in {
    MapReduce.main(Array("input", "output"))
    val outputFilePath = new Path("output/part-r-00000")
    val output = new StringBuilder("")
    val reader = new SequenceFile.Reader(new Configuration(), Reader.file(outputFilePath))

    val key = new Text()
    val value = new IntWritable()

    val outputValue = "dongguan\t2\nguangzhou\t2\njieyang\t1\nqingyuan\t1\nshenzhen\t1\nzhuhai\t1\n"

    while ( {
      reader.next(key, value)
    }) {
      output.append(key + "\t" + value + "\n")
    }

    output.mkString shouldEqual outputValue
  }

  override def beforeAll(): Unit = {
    System.setProperty("hadoop.home.dir", "/")
    clearFileSystem
  }

  override def beforeEach(): Unit = {
    val fs = FileSystem.get(new Configuration())
    val inputPath = new Path(getClass.getResource("/input.txt").getPath)
    fs.copyFromLocalFile(inputPath, new Path("input"))
  }

  override def afterEach: Unit = {
    clearFileSystem
  }

  private def clearFileSystem = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path("output"), true)
    fs.delete(new Path("input"), true)
  }
}
