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
import scala.math.abs

class FoldingClusterTask extends ClusterTask {
	override def runTask(conf: Configuration, args: Array[String]): Int = {
		val job = Job.getInstance(conf, "Folding task")
		FileInputFormat.addInputPath(job, new Path(args(0)))
		FileOutputFormat.setOutputPath(job, new Path(args(1)))
		job.setJar(conf.get("pl.gda.pg.eti.kask.kaw.jarLocation"))
		job.setMapperClass(classOf[WordCountReader])
		job.setReducerClass(classOf[FoldingReducer])
		job.setMapOutputValueClass(classOf[PairWritable])
		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[Text])
		job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

		val folds = conf.getInt("pl.gda.pg.eti.kask.kaw.folds", 10)
		var i = 0;
		for (i ← 0 to (folds - 1)) {
			MultipleOutputs.addNamedOutput(job, "set" + i.toString, classOf[TextOutputFormat[Text, Text]], classOf[Text], classOf[Text])
		}

		if (job.waitForCompletion(true)) 0 else 1
	}
}

class FoldingReducer extends Reducer[Text, PairWritable, Text, Text] {

	private var mos: MultipleOutputs[Text, Text] = null
	private var folds = 10
	private var randomPerFold = 10

	@throws[IOException]
	@throws[InterruptedException]
	override def setup(context: Reducer[Text, PairWritable, Text, Text]#Context) {
		mos = new MultipleOutputs[Text, Text](context)
		folds = context.getConfiguration.getInt("pl.gda.pg.eti.kask.kaw.folds", 10)
		randomPerFold = context.getConfiguration.getInt("pl.gda.pg.eti.kask.kaw.randomPerFold", 10)
	}

	override def reduce(key: Text, values: java.lang.Iterable[PairWritable], context: Reducer[Text, PairWritable, Text, Text]#Context): Unit = {
		if (key == null || key.toString.isEmpty) {
			return
		}
		val ran = abs(key.toString.hashCode) % (folds * randomPerFold)
		val setNumber = (ran / randomPerFold).toInt

		// TODO: do naprawienia:
		//		if(!values.exists { x => x.left.equals(CATEGORIES_ON_CLUSTER) }) {
		//		  return
		//		}
		values.foreach { x =>
			mos.write("set" + setNumber.toString, key, new Text(x.left + "\t" + x.right));
		}
	}

	override protected def cleanup(context: Reducer[Text, PairWritable, Text, Text]#Context) {
		mos.close()
	}

	private def packToString(values: java.lang.Iterable[PairWritable]): String = {
		var result = ""
		values.foreach { x ⇒
			if (result == "") {
				result = result + x.left + "\t" + x.right
			} else {
				result = result + "\t" + x.left + "\t" + x.right
			}
		}
		result
	}
}
