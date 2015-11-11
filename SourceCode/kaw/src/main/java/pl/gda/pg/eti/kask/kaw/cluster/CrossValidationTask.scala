package pl.gda.pg.eti.kask.kaw.cluster

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.LoggerFactory
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.mapreduce.Reducer
import pl.gda.pg.eti.kask.kaw.knn.CosineDistance
import org.apache.hadoop.fs.FileSystem
import java.io.InputStreamReader
import java.io.BufferedReader
import java.util.Scanner
import scala.collection.mutable.MutableList

object CrossValidationTask {
  private val logger = LoggerFactory.getLogger(classOf[CrossValidationTask])
}

class CrossValidationTask extends ClusterTask {
  override def runTask(conf: Configuration, args: Array[String]): Int = {
    val folds = conf.getInt("pl.gda.pg.eti.kask.kaw.folds", 10)
    CrossValidationTask.logger.debug("Rozpoczynam kroswalidacje, liczba foldow {}", folds)
    var i = 0;
    for (i ← 0 to folds - 1) {
      CrossValidationTask.logger.debug("Rozpoczynam fold numer {}", i)
      if (checkIfFoldHasElements(conf, i, args(0))) {
        conf.setInt("pl.gda.pg.eti.kask.kaw.currentFold", i)
        conf.set("pl.gda.pg.eti.kask.kaw.crossValidationInputFolder", args(0))

        val job = Job.getInstance(conf, "Cross Validation task");
        job.setJar("target/kaw-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
        job.setMapperClass(classOf[SetsMapper])
        job.setReducerClass(classOf[CrossValidationReducer])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[Text])
        job.setMapOutputValueClass(classOf[PairWritable])

        FileInputFormat.addInputPath(job, new Path(args(0)))
        FileOutputFormat.setOutputPath(job, new Path(args(1) + "/fold" + i.toString))

        if (!job.waitForCompletion(true)) {
          return 1
        }
      } else {
        CrossValidationTask.logger.debug("Pomijam fold {} poniewaz nie zawiera on elementow", i);
      }
    }
    0
  }

  private def checkIfFoldHasElements(conf: Configuration, currentFold: Int, inputFolder: String) = {
    val hdfs = FileSystem.get(conf)
    val files = hdfs.listStatus(new Path(inputFolder))
    files.exists { x => x.getPath.toString.contains("set" + currentFold.toString + "-") }
  }
}

object SetsMapper {
  private val logger = LoggerFactory.getLogger(classOf[SetsMapper])
}

class SetsMapper extends WordCountReader {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, PairWritable]#Context) {
    val currentFold = context.getConfiguration.getInt("pl.gda.pg.eti.kask.kaw.currentFold", -1)
    val path = context.getInputSplit.asInstanceOf[FileSplit].getPath
    val fileName = FilenameUtils.getBaseName(path.toString)
    if (currentFold == -1 || fileName.contains("set" + currentFold.toString)) {
      return
    }
    super.map(key, value, context)
  }
}

class CrossValidationReducer extends Reducer[Text, PairWritable, Text, Text] {

  private val unpacker = new PairsUnpacker
  private val distanceCounter = new CosineDistance

  override def reduce(key: Text, values: java.lang.Iterable[PairWritable], context: Reducer[Text, PairWritable, Text, Text]#Context): Unit = {
    val currentFold = context.getConfiguration.getInt("pl.gda.pg.eti.kask.kaw.currentFold", -1)
    val inputFolder = context.getConfiguration.get("pl.gda.pg.eti.kask.kaw.crossValidationInputFolder")
    val lists = unpacker.unpack(values)
    if (lists._2.isEmpty) {
      return
    }
    val hdfs = FileSystem.get(context.getConfiguration)

    val thisSetList = listThisSet(currentFold, inputFolder, hdfs)
    var endSearching = false
    val forbidden = MutableList[String]()
    while (!endSearching) {
      var foundKey: String = null
      val categories = MutableList[String]()
      val words = MutableList[Word]()
      thisSetList.foreach { path ⇒
        val file = hdfs.open(path)
        val scanner = new Scanner(file, "UTF-8")
        while (scanner.hasNextLine()) {
          val line = scanner.nextLine()
          val split = line.split("\t")
          val tempKey = split(0)
          if (foundKey == null) {
            if (!forbidden.contains(tempKey)) {
              foundKey = tempKey
              forbidden += tempKey
            }
          } else if (foundKey.equals(tempKey)) {
            if (split(1).equals(CATEGORIES_ON_CLUSTER)) {
              categories += split(2)
            } else {
              words += new Word(split(1), split(2).toInt)
            }
          }
        }
        file.close
      }
      if (foundKey == null) {
        endSearching = true
      } else {
        val dist = distanceCounter.getDistance(lists._1, words.toList)
        val foldingFunc: ((String, String) ⇒ String) = { (str, cat) ⇒ str + "," + cat }
        val fileName = foundKey
        val keyStr = categories.foldLeft(fileName + "[")(foldingFunc) + "]"
        val resultStr = lists._2.foldLeft(key.toString() + "[" + dist.toString + ",")(foldingFunc) + "]"
        context.write(new Text(keyStr), new Text(resultStr))
      }
    }
  }

  private def listThisSet(currentFold: Int, inputFolder: String, hdfs: FileSystem): List[Path] = {
    val files = hdfs.listStatus(new Path(inputFolder))
    var paths = List[Path]()
    var i = 0
    for (i ← 0 to files.length - 1) {
      val filePath = files(i).getPath
      if (filePath.toString.contains("set" + currentFold.toString + "-")) {
        paths = paths :+ filePath
      }
    }
    paths
  }

  private def unpackArticleFromLine(line: String): Tuple2[List[Word], List[String]] = {
    var words = List[Word]()
    var cats = List[String]()
    val split = line.toString().split("\t")
    val newKey = new Text(split(0))
    var i = 1
    for (i ← 1 to split.length) {
      if (split(i).equals(CATEGORIES_ON_CLUSTER)) {
        cats = cats :+ split(i + 1)
      } else {
        words = words :+ new Word(split(i), split(i + 1).toInt)
      }
    }
    (words, cats)
  }
}