package pl.gda.pg.eti.kask.kaw.cluster

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
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import java.io.IOException
import scala.util.Random

class FoldingClusterTask extends ClusterTask {
	override def runTask(conf: Configuration, args: Array[String]): Int = {
		val job = Job.getInstance(conf, "Word count in Scala");
		job.setJar("target/kaw-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
		//job.setJarByClass(classOf[WordCountTask])
		job.setMapperClass(classOf[WordCountReader])
		job.setReducerClass(classOf[FoldingReducer])
		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[Text])
		FileInputFormat.addInputPath(job, new Path(args(0)))
		MultipleOutputs.addNamedOutput(job, "trainingSet", classOf[FileOutputFormat[Text, Text]], classOf[Text], classOf[Text])
		MultipleOutputs.addNamedOutput(job, "testingSet", classOf[FileOutputFormat[Text, Text]], classOf[Text], classOf[Text])
		FileOutputFormat.setOutputPath(job, new Path(args(1)))
		if (job.waitForCompletion(true)) 0 else 1
	}
}

class FoldingMapper extends Mapper[Object, Text, IntWritable, Word] {

	private val parser = new MatrixuOutputParser
	private val objOut = new ObjectWritable
	private val idOut = new IntWritable

	override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Word]#Context): Unit = {
		//		parser.parse(value.toString, context)
	}
}

class FoldingReducer extends Reducer[Text, PairWritable, Text, Text] {

	private var mos: MultipleOutputs[Text, Text] = null
	private var allSet = 9
	private var testingSet = 1

	@throws [IOException]
	@throws [InterruptedException]
	override def setup(context: Reducer[Text, PairWritable, Text, Text]#Context) {
		mos = new MultipleOutputs[Text, Text](context)
		allSet = context.getConfiguration.getInt("pl.gda.pg.eti.kask.kaw.allSet", 1)
		testingSet = context.getConfiguration.getInt("pl.gda.pg.eti.kask.kaw.testingSet", 9)
	}

	override def reduce(key: Text, values: java.lang.Iterable[PairWritable], context: Reducer[Text, PairWritable, Text, Text]#Context): Unit = {
		
		val ran = new Random().nextInt(allSet)
		if(ran < testingSet) {
			
			// TODO: mos.write("testingSet", , );
		}
		else {
			// TODO: mos.write("trainingSet", , );
		}
	}
}