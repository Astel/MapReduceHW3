package com.epam.mapreduce.types

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Writable

class Record(var count: Int, var os: String) extends Writable{
  def this() {
    this(0, "none")
  }

  def getCount: Int = count

  def getOs: String = os

  override def readFields(dataInput: DataInput): Unit = {
    count = dataInput.readInt()
    os = dataInput.readUTF()
  }

  override def write(dataOutput: DataOutput): Unit = {
    dataOutput.writeInt(count)
    dataOutput.writeUTF(os)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Record]

  override def equals(other: Any): Boolean = other match {
    case that: Record =>
      (that canEqual this) &&
        count == that.count &&
        os == that.os
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(count, os)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"$count\t$os"
}
