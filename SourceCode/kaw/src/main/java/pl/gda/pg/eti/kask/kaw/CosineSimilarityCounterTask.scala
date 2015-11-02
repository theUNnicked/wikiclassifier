package pl.gda.pg.eti.kask.kaw

import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import pl.gda.pg.eti.kask.kaw.extract.MatrixuOutputParser
import pl.gda.pg.eti.kask.kaw.knn.CosineSimilarityIndexCounter

@deprecated(message = "Nie uzywany")
class CosineSimilarityCounterTask extends ClusterTask {
  override def runTask(conf: Configuration, args: Array[String]): Int = {
		val job = Job.getInstance(conf, "Word count in Scala");
		job.setJar("target/kaw-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
		//job.setJarByClass(classOf[WordCountTask])
		job.setMapperClass(classOf[TokenizerMapper])
		job.setCombinerClass(classOf[IntSumReducer])
		job.setReducerClass(classOf[IntSumReducer])
		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[IntWritable])
		FileInputFormat.addInputPath(job, new Path(args(0)))
		FileOutputFormat.setOutputPath(job, new Path(args(1)))
		if (job.waitForCompletion(true)) 0 else 1
	}
}

@deprecated(message = "Nie uzywany")
class CosineMapper extends Mapper[Object, Text, IntWritable, Word] {

	private val parser = new MatrixuOutputParser
	private val objOut = new ObjectWritable
	private val idOut = new IntWritable

	override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Word]#Context): Unit = {
//		parser.parse(value.toString, context)
	}
}

@deprecated(message = "Nie uzywany")
class CosineReducer extends Reducer[IntWritable, Word, Text, IntWritable] {

	private val counter = new CosineSimilarityIndexCounter
	
	override def reduce(key: IntWritable, values: java.lang.Iterable[Word], context: Reducer[IntWritable, Word, Text, IntWritable]#Context): Unit = {
		//counter.countSimilarity(key.get, values, context)
	}
}