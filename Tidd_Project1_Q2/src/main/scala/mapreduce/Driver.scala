package mapreduce

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object Driver extends App{

  if(args.length != 2){
    print("Usage: Q1Driver <input dir> <output dir>")
    System.exit(-1)
  }

  // Instantiating Job object
  val job = Job.getInstance()

  // Set the jar file that contains driver, mapper, and reducer
  // This jar file will be transferred to nodes that run tasks
  job.setJarByClass(Driver.getClass)

  // Specify the job name
  job.setJobName("Project1 Q2")

  // Specify the input and output paths based on args
  FileInputFormat.setInputPaths(job, new Path(args(0)))
  FileOutputFormat.setOutputPath(job, new Path(args(1)))


  // Specify the Mapper and Reducer
  job.setMapperClass(classOf[Q2Mapper])
  job.setReducerClass(classOf[Q2Reducer])


  // Specify the job's output key and value classes
  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[IntWritable])

  val success = job.waitForCompletion(true)
  System.exit(if (success) 0 else 1)

}
