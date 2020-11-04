package mapreduce

import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

class Q3Reducer extends Reducer[Text, MaxLinkWritable, Text, MaxLinkWritable]{

  override def reduce(key: Text, values: lang.Iterable[MaxLinkWritable], context: Reducer[Text, MaxLinkWritable, Text, MaxLinkWritable]#Context): Unit = {
    var link: String = ""
    var count: Int = -1
    for(value <- values){
      if(value.getCountInt() > count){
        count = value.getCountInt()
        link = value.getLinkString()
      }
    }
    context.write(key,new MaxLinkWritable(new Text(link), new IntWritable(count)))
  }
}
