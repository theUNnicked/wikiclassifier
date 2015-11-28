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
		val job = Job.getInstance(conf, "Cross Validation average score counter task");
		job.setJar(conf.get("pl.gda.pg.eti.kask.kaw.jarLocation"))
		job.setMapperClass(classOf[CVACounterMapper])
		job.setReducerClass(classOf[CVACounterReducer])
		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[Text])
		job.setMapOutputValueClass(classOf[Text])

		FileInputFormat.addInputPath(job, new Path(args(0)))
		FileOutputFormat.setOutputPath(job, new Path(args(1)))

		if (!job.waitForCompletion(true)) 0 else 1
	}

}

class CVACounterMapper extends Mapper[Object, Text, Text, Text] {

	override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context) {
		val valStr = value.toString
		if(value == null || valStr.isEmpty) {
			return
		}

		val allValuesStr = valStr.substring(valStr.indexOf('\t') + 1)
		val allValuesArr = allValuesStr.split("\t")

		val TP = allValuesArr(0)
		val FP = allValuesArr(1)
		val FN = allValuesArr(2)
		val sensitivity = allValuesArr(3)
		val precision = allValuesArr(4)
		val fMeasure = allValuesArr(5)

		val allValsRes = TP + "\t" + FP + "\t" + FN + "\t" + sensitivity + "\t" + precision + "\t" + fMeasure
		context.write(new Text("all"), new Text(allValsRes))
	}

}

class CVACounterReducer extends Reducer[Text, Text, Text, Text] {

	override def reduce(key: Text, values: java.lang.Iterable[Text], context:Reducer[Text, Text, Text, Text]#Context ) {

		val sum = values.foldLeft((0, 0, 0, 0.0, 0.0, 0.0, 0)) { (prev, n) =>
			val allValuesArr = n.toString.split("\t")
			val TP = allValuesArr(0).toInt
			val FP = allValuesArr(1).toInt
			val FN = allValuesArr(2).toInt
			val sensitivity = allValuesArr(3).toDouble
			val precision = allValuesArr(4).toDouble
			val fMeasure = allValuesArr(5).toDouble

			(prev._1 + TP, prev._2 + FP, prev._3 + FN, prev._4 + sensitivity, prev._5 + precision, prev._6 + fMeasure, prev._7 + 1)
		}

		val size = sum._7
		val TP = sum._1
		val FP = sum._2
		val FN = sum._3
		val sensitivity = sum._4 / size
		val precision = sum._5 / size
		val fMeasure = sum._6 / size

		val allValsRes = TP.toString + "\t" + FP.toString + "\t" + FN.toString + "\t" + sensitivity.toString + "\t" + precision.toString + "\t" + fMeasure.toString

		context.write(new Text("Average score:"), new Text(allValsRes))
	}
}
