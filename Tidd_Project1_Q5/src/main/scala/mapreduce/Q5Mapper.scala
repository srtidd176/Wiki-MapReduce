package mapreduce

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class Q5Mapper extends Mapper[LongWritable, Text, Text, IntWritable] {

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val line = value.toString
    line.split(" ").filter(_.length>0) match {
      case Array(domain, page_title, count_views, response_size) => if(domain == "en" || domain == "en.m") context.write(new Text(page_title),new IntWritable(count_views.toInt))
      case _ => println(s"COULD NOT INTERPRET: ${line}")
    }
  }

}
