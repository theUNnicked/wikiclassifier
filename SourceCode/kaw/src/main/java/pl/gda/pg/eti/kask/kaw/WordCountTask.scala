package pl.gda.pg.eti.kask.kaw

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.commons.io.FilenameUtils

class WordCountTask extends ClusterTask {
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

class TokenizerMapper extends Mapper[Object, Text, Text, Text] {

	private val out = new Text
	private val word = new Text

	override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
		val regex = """([a-zA-ZżźćńółęąśŻŹĆĄŚĘŁÓŃ]*)|(([0-9]{4})-([0-9]{2})-([0-9]{2})|([0-9]{4}))""".r
		val path = context.getInputSplit.asInstanceOf[FileSplit].getPath
		var valueString = value.toString
		var fileName = FilenameUtils.getBaseName(path.toString)
		val cutted = cutCategories(valueString)
		cutted._1.foreach { category ⇒
			val text = new Text
			text.set(fileName + "\\\\:Cat")
			out.set(category)
			context.write(text, out)
		}
		valueString = cutted._2
		regex.findAllIn(valueString).foreach { word ⇒
			val text = new Text
			text.set(fileName + "\\" + word)
			out.set("1")
			context.write(text, out)
		}
	}

	private def cutCategories(text: String): Tuple2[List[String], String] = {
		val reg = """(\[\[Kategoria:.+\]\])""".r
		val all = reg.findAllIn(text)
		var categories = List[String]()
		all.foreach { x ⇒ categories = getCategoryAndInsert(x, categories) }
		val newText = reg.replaceAllIn(text, "")
		return (categories, newText)
	}

	private def getCategoryAndInsert(categoryString: String, categories: List[String]): List[String] = {
		val category = categoryString.replace("[[Kategoria:", "").replace("]]", "").trim
		return categories :+ category
	}
}

class IntSumReducer extends Reducer[Text, Text, Text, Text] {

	override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
		var keyString = key.toString
		if (keyString.contains("\\\\:Cat")) {
			values.foreach { value ⇒
				val title = keyString.replace("\\\\:Cat", "")
				val fullValue = "::Cat\t" + value
				context.write(new Text(title), new Text(fullValue))
			}
		}
		else {
			val sum = values.foldLeft(0) { (sum, v) ⇒ sum + v.toString.toInt }
			val split = keyString.toString.split("\\")
			context.write(new Text(split(0)), new Text(split(1) + sum.toString))
		}
	}
}
