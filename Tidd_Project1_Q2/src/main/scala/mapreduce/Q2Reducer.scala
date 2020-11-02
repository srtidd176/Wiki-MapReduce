package mapreduce

import java.lang

import org.apache.hadoop.io.{FloatWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

class Q2Reducer extends Reducer[Text,IntWritable, Text, IntWritable]{

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    var numLinks = 0
    values.forEach(numLinks+=_.toString.toInt)
    context.write(new Text(key), new IntWritable(numLinks))
  }
}
