package pl.gda.pg.eti.kask.kaw.cluster

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
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.FileSystem
import pl.gda.pg.eti.kask.kaw.variates.Dictionary

class WordCountTask extends ClusterTask {
	override def runTask(conf: Configuration, args: Array[String]): Int = {
		val job = Job.getInstance(conf, "Word count task");
		job.setJar(conf.get("pl.gda.pg.eti.kask.kaw.jarLocation"))
		//job.setJarByClass(classOf[WordCountTask])
		job.setMapperClass(classOf[TokenizerMapper])
		job.setCombinerClass(classOf[IntSumCombiner])
		job.setReducerClass(classOf[IntSumReducer])
		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[Text])
		FileInputFormat.addInputPath(job, new Path(args(0)))
		FileOutputFormat.setOutputPath(job, new Path(args(1)))
		if (job.waitForCompletion(true)) 0 else 1
	}
}

object TokenizerMapper {
	private val logger = LoggerFactory.getLogger(classOf[TokenizerMapper])
	private var dictionary: Dictionary = null

	def findLexem(word: String, configuration: Configuration) = {
		if (dictionary == null) {
			val hdfs = FileSystem.get(configuration)
			dictionary = new Dictionary()
			dictionary.loadDictionary(hdfs.open(new Path(configuration.get("pl.gda.pg.eti.kask.kaw.dictionaryLocation"))))
		}
		dictionary.getWordLexeme(word)
	}

	def disposeDictionary {
	  dictionary = null
	  System.gc()
	}
}

class TokenizerMapper extends Mapper[Object, Text, Text, Text] {

	private val text = new Text
	private val out = new Text

	override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {

	  val wrongWords = """(\[\[:{0,1}Grafika:.+\]\])|(\[\[:{0,1}Media:.+\]\])|(\{\{Przypisy)|(\{\{Uwagi)|(\{\{SORTUJ:.+}})|(\{\{subst:.+}})|(\{\{Dokumentacja)|(<ref>|<\/ref>|<small>|<\/small>|<del>|<\/del>|<sup>|<\/sup>|<sub>|<\/sub>|<code>|<\/code>|<math>.+<\/math>|<nowiki>|<\/nowiki>|<includeonly>|<\/includeonly>|<noinclude>|<\/noinclude>)(\{\{Ujednoznacznienie}})|(\{\{Dokumentacja}})|(http:\/\/[a-zA-z0-9._+/\-?&=]+)|(\[\[Plik:.+\]\])|(\#PATRZ.+\[\[.+\]\])|(\{\{Inne znaczenia)|(\{\{Ujednoznacznienie)""".r
		val valueString = wrongWords.replaceAllIn(value.toString, "")
		val wordsReg = """([a-zA-ZżźćńółęąśŻŹĆĄŚĘŁÓŃÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿŒœŠšŸ]{2,})|(([0-9]{4})-([0-9]{2})-([0-9]{2}))|([0-9]{4})""".r
		val path = context.getInputSplit.asInstanceOf[FileSplit].getPath
		val fileName = FilenameUtils.getBaseName(path.toString)
		val cutted = cutCategories(valueString)
		cutted._1.foreach { category ⇒
			text.set(fileName + "\\\\:Cat")
			out.set(category)
			context.write(text, out)
		}
		wordsReg.findAllIn(cutted._2).foreach { word ⇒
			text.set(fileName + "\\" + TokenizerMapper.findLexem(word, context.getConfiguration))
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
		val regToDelete = """(\{\{.*\}\})""".r
		val category = regToDelete.replaceAllIn(categoryString, "").replace("[[Kategoria:", "").replace("]]", "").replace("\t", " ").trim
		val index = category.indexOf("|")
		val categoryTrimmed = if (index < 0) category else category.substring(0, index)
		return categories :+ categoryTrimmed
	}
}

class IntSumCombiner extends Reducer[Text, Text, Text, Text] {
	override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
		val keyString = key.toString
		if (!keyString.contains(":Cat")) {
			val sum = values.foldLeft(0) { (sum, v) ⇒ sum + v.toString.toInt }
			context.write(new Text(key), new Text(sum.toString))
		}
		else {
			values.foreach { x ⇒ context.write(key, x) }
		}
	}
}

class IntSumReducer extends Reducer[Text, Text, Text, Text] {

	override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
		var keyString = key.toString
		if (keyString.contains("\\\\:Cat")) {
			val title = keyString.replace("\\\\:Cat", "")
			values.foreach { x =>
			  val fullValue = CATEGORIES_ON_CLUSTER + "\t" + x
			  context.write(new Text(title), new Text(fullValue))
			}
		}
		else {
			val sum = values.foldLeft(0) { (sum, v) ⇒ sum + v.toString.toInt }
			val split = keyString.split("\\\\")
			if (split.length == 2) {
				context.write(new Text(split(0)), new Text(split(1) + "\t" + sum.toString))
			}
		}
	}
}
