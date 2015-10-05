package pl.gda.pg.eti.kask.kaw

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


class CountStringTask extends ClusterTask {
	override def runTask(conf: Configuration, args: Array[String]): Int = {
		val job = Job.getInstance(conf, "Count as string app");
		job.setJar("target/kaw-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
		job.setMapperClass(classOf[CounterTokenizerMapper])
		job.setCombinerClass(classOf[CounterIntSumReducer])
		job.setReducerClass(classOf[CounterIntSumReducer])
		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[Text])
		FileInputFormat.addInputPath(job, new Path(args(0)))
		FileOutputFormat.setOutputPath(job, new Path(args(1)))
		if (job.waitForCompletion(true)) 0 else 1
	}
}

class CounterTokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

	private val one = new IntWritable(1)
	private val word = new Text

	override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
		value.toString.split("\\s+").map { t =>
			word.set(t)
			context.write(word, one)
		}
	}
}

class CounterIntSumReducer extends Reducer[Text, IntWritable, Text, Text] {

	override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
		context.write(key, new Text(new Counter().count(values.size)))
	}
}

class Counter {
	
	def count(size: Int) : String = {
		val strings = Array("aa", "bb", "cc", "dd", "ee")
		return size.toString + " " + strings(size % strings.length)
	}
	
}
