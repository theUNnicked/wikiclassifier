package pl.gda.pg.eti.kask.kaw.cluster

import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput
import org.apache.hadoop.fs.FileSystem
import java.io.BufferedReader
import java.io.InputStreamReader
import pl.gda.pg.eti.kask.kaw.knn.CosineDistance
import org.apache.hadoop.io.Text
import java.util.Scanner
import scala.collection.mutable.MutableList

class NoMatrixuSimilarityTask extends ClusterTask {
	override def runTask(conf: Configuration, args: Array[String]): Int = {
		val job = Job.getInstance(conf, "kNN Similarity Task");
		job.setJar("target/kaw-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
		//job.setJarByClass(classOf[WordCountTask])
		job.setMapperClass(classOf[WordCountReader])
		job.setReducerClass(classOf[KnnReducer])
		job.setMapOutputValueClass(classOf[PairWritable])
		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[Text])
		FileInputFormat.addInputPath(job, new Path(args(0)))
		FileOutputFormat.setOutputPath(job, new Path(args(1)))
		if (job.waitForCompletion(true)) 0 else 1
	}
}

class Word(private var word: String, private var count: Int) {
	def getWord = { word }
	def getCount = { count }
}

class PairWritable(var left: String, var right: String) extends Writable {

	def getLeft = { left }
	def getRight = { right }

	def setLeft(left: String) { this.left = left }
	def setRight(right: String) { this.right = right }

	def this() {
		this("", "")
	}

	override def readFields(in: DataInput): Unit = {
		val txt = new Text()
		txt.readFields(in)
		left = txt.toString()

		val txt2 = new Text()
		txt2.readFields(in)
		right = txt2.toString()
	}

	override def write(out: DataOutput): Unit = {
		new Text(left).write(out)
		new Text(right).write(out)
	}
}

class WordCountReader extends Mapper[Object, Text, Text, PairWritable] {

	private val text = new Text
	private val out = new Text

	override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, PairWritable]#Context): Unit = {
		val parts = value.toString.split("\t")
		if (parts(1).equals(CATEGORIES_ON_CLUSTER)) {
			var i = 2
			while (i < parts.length) {
				context.write(new Text(parts(0)), new PairWritable(parts(1), parts(i)))
				i = i + 1
			}
		}
		else {
			context.write(new Text(parts(0)), new PairWritable(parts(1), parts(2)))
		}
	}
}

object KnnReducer {
	private val distanceCounter = new CosineDistance

	private var newArticleLists: Tuple2[List[Word], List[String]] = null

	def getNewArticleLists(conf: Configuration): Tuple2[List[Word], List[String]] = {
		if (newArticleLists == null) {
			newArticleLists = new NewArticleUnpacker(conf.get("pl.gda.pg.eti.kask.kaw.newArticleOutput"), conf).unpack()
		}
		newArticleLists
	}

}

class KnnReducer extends Reducer[Text, PairWritable, Text, Text] {

	private val unpacker = new PairsUnpacker

	override def reduce(key: Text, values: java.lang.Iterable[PairWritable], context: Reducer[Text, PairWritable, Text, Text]#Context): Unit = {
		val lists = unpacker.unpack(values)
		if (lists._2.isEmpty) {
			return
		}
		val newArticleLists = KnnReducer.getNewArticleLists(context.getConfiguration)
		val dist = KnnReducer.distanceCounter.getDistance(lists._1, newArticleLists._1)

		val resultStr = lists._2.foldLeft(dist.toString()) { (str, cat) ⇒ str + "\t" + cat }
		val result = new Text(resultStr)

		context.write(key, result)
	}
}

class PairsUnpacker {
	def unpack(values: java.lang.Iterable[PairWritable]): Tuple2[List[Word], List[String]] = {
		val words = MutableList[Word]()
		val categories = MutableList[String]()

		values.foreach { x ⇒
			if (x.left.equals(CATEGORIES_ON_CLUSTER)) {
				categories += x.right
			}
			else {
				words += new Word(x.left, x.right.toInt)
			}
		}

		(words.toList, categories.toList)
	}
}

class NewArticleUnpacker(private val outputDir: String, private val conf: Configuration) {

	def unpack(): Tuple2[List[Word], List[String]] = {
		val words = MutableList[Word]()
		val categories = List[String]()

		val hdfs = FileSystem.get(conf)

		val files = hdfs.listStatus(new Path(outputDir));
		var i = 0

		for (i ← 0 to files.length - 1) {
			if (!files(i).getPath.toString.contains(".crc")) {
				val file = hdfs.open(files(i).getPath)
				val scanner = new Scanner(file, "UTF-8");
				while (scanner.hasNextLine()) {
					val value = scanner.nextLine()
					val parts = value.toString.split("\t")
					if (!parts(1).equals(CATEGORIES_ON_CLUSTER)) {
						words += new Word(parts(1), parts(2).toInt)
					}

				}
				file.close()
				scanner.close()
			}
		}
		(words.toList, categories)
	}

}
