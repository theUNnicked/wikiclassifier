package pl.gda.pg.eti.kask.kaw.cluster

import scala.collection.JavaConversions._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.mapreduce.Reducer
import pl.gda.pg.eti.kask.kaw.knn.KNearestNeighboursExtract
import pl.gda.pg.eti.kask.kaw.knn.NearestNeighboursCategoryExtractor
import pl.gda.pg.eti.kask.kaw.CategorizationApplicationObject
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.fs.FileSystem

class CrossValidationResultsTask extends ClusterTask {

	def runTask(conf: Configuration, args: Array[String]): Int = {
		val hdfs = FileSystem.get(conf)
		val stat = hdfs.listStatus(new Path(args(0)))

		val job = Job.getInstance(conf, "Cross Validation score counter task");
		job.setJar("target/kaw-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
		job.setMapperClass(classOf[CrossValidationResultsMapper])
		job.setReducerClass(classOf[CrossValidationResultsReducer])
		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[DoubleWritable])
		job.setMapOutputValueClass(classOf[Text])

		stat.foreach { x ⇒
			val str = x.getPath.toString
			if (str.contains("fold")) {
				val dirName = FilenameUtils.getBaseName(str)
				FileInputFormat.addInputPath(job, new Path(args(0) + "/" + dirName))
			}
		}

		FileOutputFormat.setOutputPath(job, new Path(args(1)))
		if (job.waitForCompletion(true)) 0 else 1

	}
}

class CrossValidationResultsMapper extends Mapper[Object, Text, Text, Text] {
	override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context) {
		if (value == null || value.toString.isEmpty) {
			return
		}
		val line = value.toString
		context.write(new Text(line.substring(0, line.indexOf('['))), value)
	}
}

class CrossValidationResultsReducer extends Reducer[Text, Text, Text, DoubleWritable] {
	override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, DoubleWritable]#Context) {
		val expectedCategories = extractExpectedCategories(values.head.toString)
		val k = context.getConfiguration.getInt("pl.gda.pg.eti.kask.kaw.kNeighbours", 15)
		val neighbours = extractBestKNeighbours(k, values)
		val categories = extractPredictedCategories(neighbours)

		val value = new DoubleWritable
		value.set(compare(expectedCategories, categories))
		context.write(key, value)
	}

	private def extractPredictedCategories(kNeighbours: Array[String]) = {
		val extractor = new NearestNeighboursCategoryExtractor
		val extractCategoriesWithSimilarityFromString = { s: String ⇒
			val predictionString = extractPredictionString(s)
			val split = predictionString.substring(predictionString.indexOf('[') + 1, predictionString.indexOf(']')).split(",")
			(split(0).toDouble, split.takeRight(split.length - 1).toList)
		}
		val passedByThresholdingStrategy = CategorizationApplicationObject.strategyBest70Percent
		extractor.extractCategories(kNeighbours, extractCategoriesWithSimilarityFromString, passedByThresholdingStrategy)
	}

	private def extractBestKNeighbours(k: Int, values: java.lang.Iterable[Text]) = {
		val extractor = new KNearestNeighboursExtract(k)
		val hasElement = { (iter: AnyRef, id: Int) ⇒ iter.asInstanceOf[java.util.Iterator[Text]].hasNext }
		val takeElement = { (iter: AnyRef, id: Int) ⇒ iter.asInstanceOf[java.util.Iterator[Text]].next.toString }
		val extractSimilarity = { s: String ⇒
			val prediction = extractPredictionString(s)
			prediction.substring(prediction.indexOf('[') + 1, prediction.indexOf(',')).toDouble
		}
		extractor.extractKNearestNeighbours(null, values.iterator, hasElement, takeElement, extractSimilarity)
	}

	private def extractPredictionString(line: String) = {
		line.substring(line.indexOf('\t') + 1)
	}

	private def extractExpectedCategories(line: String) = {
		try {
			line.substring(line.indexOf('[') + 1, line.indexOf(']')).split(",").toList
		} catch {
			case e: Exception ⇒
				println(line)
				println(e.getMessage)
				e.printStackTrace()
				null
		}
	}

	private def compare(expectedCategories: List[String], predictedCategories: List[String]): Double = {
		var n = 0
		expectedCategories.foreach { expected ⇒
			if (predictedCategories.contains(expected)) {
				n += 1
			}
		}
		((n.toDouble * 2) / (expectedCategories.length + predictedCategories.length))
	}
}