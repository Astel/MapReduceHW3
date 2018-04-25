package com.epam.mapreduce

import com.epam.mapreduce.types.Record
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.util.{Tool, ToolRunner}

object MapReduce extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    val job = Job.getInstance(getConf)
    job.addCacheFile(getClass.getResource("/city_en.txt").toURI)

    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[Map])
    job.setCombinerClass(classOf[Combiner])
    job.setPartitionerClass(classOf[OSPartitioner])
    job.setReducerClass(classOf[Reduce])

    job.setOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Record])
    job.setOutputValueClass(classOf[IntWritable])

    job.setInputFormatClass(classOf[TextInputFormat])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[Text, IntWritable]])
    if (job.waitForCompletion(true)) 0 else 1
  }

  def main(args: Array[String]): Unit = {
    val res = ToolRunner.run(new Configuration(), MapReduce, args)
  }
}
