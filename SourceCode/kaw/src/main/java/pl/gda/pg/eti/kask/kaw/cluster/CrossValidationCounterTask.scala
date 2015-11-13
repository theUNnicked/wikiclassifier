package pl.gda.pg.eti.kask.kaw.cluster

import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.mapreduce.Reducer


class CrossValidationAverageCounterTask extends ClusterTask {

	def runTask(conf: Configuration, args: Array[String]): Int = {
		val job = Job.getInstance(conf, "Cross Validation task");
		job.setJar("target/kaw-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
		job.setMapperClass(classOf[CVACounterMapper])
		job.setCombinerClass(classOf[CVACounterReducer])
		job.setReducerClass(classOf[CVACounterReducer])
		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[DoubleWritable])
		job.setMapOutputValueClass(classOf[DoubleWritable])

		FileInputFormat.addInputPath(job, new Path(args(0)))
		FileOutputFormat.setOutputPath(job, new Path(args(1)))

		if (!job.waitForCompletion(true)) 0 else 1
	}

}

class CVACounterMapper extends Mapper[Object, Text, Text, DoubleWritable] {
	
	override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, DoubleWritable]#Context) {
		val valStr = value.toString
		if(value == null || valStr.isEmpty) {
			return
		}
		val doubleWritable = new DoubleWritable
		doubleWritable.set(valStr.substring(valStr.indexOf('\t') + 1).toDouble)
		context.write(new Text("all"), doubleWritable)
	}
	
}

class CVACounterReducer extends Reducer[Text, DoubleWritable, Text, DoubleWritable] {
	
	override def reduce(key: Text, values: java.lang.Iterable[DoubleWritable], context:Reducer[Text, DoubleWritable, Text, DoubleWritable]#Context ) {
		val sum = values.foldLeft((0.0, 0.0)) { (prev, n) => (prev._1 + n.get, prev._2 + 1.0) }
		val avg = sum._1 / sum._2
		val doubleWritable = new DoubleWritable
		doubleWritable.set(avg)
		context.write(new Text("average"), doubleWritable)
	}
}