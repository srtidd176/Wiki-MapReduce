package mapreduce

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{IntWritable, Text, Writable}

class MaxLinkWritable(link: Text, count: IntWritable) extends Writable{

  def this()={
    this(new Text(""), new IntWritable(-1))
  }

  override def write(out: DataOutput): Unit = {
    link.write(out)
    count.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    link.readFields(in)
    count.readFields(in)
  }
  def getLink(): Text = {
    link
  }

  def getCount(): IntWritable = {
    count
  }

  def getLinkString(): String = {
    link.toString
  }

  def getCountInt(): Int = {
    count.toString.toInt
  }

  override def toString: String = {
    s"${link}"+"\t"+s"${count}"
  }
}
