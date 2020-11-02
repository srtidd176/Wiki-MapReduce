package mapreduce

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class Q2Mapper extends Mapper[LongWritable, Text, Text, IntWritable] {

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val line = value.toString
    line.split("\t").filter(_.length>0) match {
      case Array(prev, curr, typeLink, n) => if (typeLink =="link"){context.write(new Text(prev),new IntWritable(n.toInt))}
      case _ => println(s"COULD NOT INTERPRET: ${line}")
    }
  }
}