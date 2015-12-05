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

		val job = Job.getInstance(conf, "Cross Validation score counter task")
		job.setJar(conf.get("pl.gda.pg.eti.kask.kaw.jarLocation"))
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

class CrossValidationResultsReducer extends Reducer[Text, Text, Text, Text] {
	override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context) {
		val vhead = values.head.toString
		val expectedCategories = extractExpectedCategories(vhead)
		val k = context.getConfiguration.getInt("pl.gda.pg.eti.kask.kaw.kNeighbours", 15)
		val neighbours = extractBestKNeighbours(k, values)
		try {
			val categories = extractPredictedCategories(neighbours, context.getConfiguration)
			// TP, FP, FN
			val compared = compare(expectedCategories, categories)

			val TP = compared._1
			val FP = compared._2
			val FN = compared._3

			if(TP + FN == 0 || TP + FP == 0) {
				return;
			}

			val sensitivity = TP.toDouble / (TP.toDouble + FN.toDouble)
			val precision = TP.toDouble / (TP.toDouble + FP.toDouble)
			val recall = sensitivity
			val fMeasure = if(precision + recall == 0.0) 0.0 else ((2 * precision * recall) / (precision + recall))

			val allValues = new Text(TP.toString + "\t" + FP.toString + "\t" + FN.toString + "\t" + sensitivity.toString + "\t" + precision.toString + "\t" + fMeasure.toString)
			context.write(key, allValues)
		}
		catch {
			case e: Exception =>
				println(vhead)
				println(neighbours)
				e.printStackTrace();
		}
	}

	private def extractPredictedCategories(kNeighbours: Array[String], conf: Configuration) = {
		val extractor = new NearestNeighboursCategoryExtractor
		val extractCategoriesWithSimilarityFromString = { s: String ⇒
			val predictionString = extractPredictionString(s)
			val split = predictionString.substring(predictionString.indexOf('[') + 1, predictionString.indexOf(']')).split(",")
			(split(0).toDouble, split.takeRight(split.length - 1).toList)
		}
		val passedByThresholdingStrategy = CategorizationApplicationObject.getStrategy(conf)
		extractor.extractCategories(kNeighbours, extractCategoriesWithSimilarityFromString, passedByThresholdingStrategy)
	}

	private def extractBestKNeighbours(k: Int, values: java.lang.Iterable[Text]) = {
		val extractor = new KNearestNeighboursExtract(k)
		val hasElement = { (iter: AnyRef, id: Int) ⇒ iter.asInstanceOf[java.util.Iterator[Text]].hasNext }
		val takeElement = { (iter: AnyRef, id: Int) ⇒ iter.asInstanceOf[java.util.Iterator[Text]].next.toString }
		val extractSimilarity = { s: String ⇒
			try {
				val prediction = extractPredictionString(s)
				prediction.substring(prediction.indexOf('[') + 1, prediction.indexOf(',')).toDouble
			}
			catch {
				case e: Exception => 0.0
			}
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

	private def compare(expectedCategories: List[String], predictedCategories: List[String]): Tuple3[Int, Int, Int] = {
		var TP = 0
		var FP = 0
		var FN = 0

		expectedCategories.foreach { expected ⇒
			if (predictedCategories.contains(expected)) {
				TP += 1
			}
			else {
				FN += 1
			}
		}

		predictedCategories.foreach { predicted =>
			if(!expectedCategories.contains(predicted)) {
				FP += 1
			}
		}

		(TP, FP, FN)
	}
}
