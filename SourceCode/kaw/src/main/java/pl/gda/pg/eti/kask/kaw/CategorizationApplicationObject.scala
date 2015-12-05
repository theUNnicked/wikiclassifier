package pl.gda.pg.eti.kask.kaw

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration
import java.security.PrivilegedExceptionAction
import pl.gda.pg.eti.kask.kaw.dump.ArticleReader
import pl.gda.pg.eti.kask.kaw.dump.DistributedArticleReader
import org.apache.hadoop.fs.FileSystem
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.Path
import java.io.File
import java.util.Properties
import pl.gda.pg.eti.kask.kaw.knn.CosineSimilarityIndexCounter
import pl.gda.pg.eti.kask.kaw.cluster.WordCountTask
import pl.gda.pg.eti.kask.kaw.cluster.NoMatrixuSimilarityTask
import pl.gda.pg.eti.kask.kaw.grade.CrossValidation
import pl.gda.pg.eti.kask.kaw.cluster.FoldingClusterTask
import pl.gda.pg.eti.kask.kaw.cluster.CrossValidationTask
import java.io.FileInputStream
import java.util.Scanner
import pl.gda.pg.eti.kask.kaw.cluster.NoMatrixuSimilarityTask
import pl.gda.pg.eti.kask.kaw.cluster.TokenizerMapper
import pl.gda.pg.eti.kask.kaw.cluster.CrossValidationResultsTask
import pl.gda.pg.eti.kask.kaw.cluster.CrossValidationAverageCounterTask
import pl.gda.pg.eti.kask.kaw.cluster.ClusterTask
import scala.collection.JavaConversions._
import scala.collection.mutable.MutableList

class WrongUsageException extends Exception("Wrong usage, check your parameters and try again");
class InvalidPropertiesException extends Exception("Wrong parameters, check parameter file [kaw.properties]");

class CategorizationApplicationObject {}

object CategorizationApplicationObject {

  private var percentage = -1.0
  
  def getStrategy(conf: Configuration) = {
    if(percentage < 0) {
      percentage = conf.getDouble("pl.gda.pg.eti.kask.kaw.classifierPrecision", 0.8)
    }
    strategyBestPercent
  }
  
	val strategyBestPercent: (Double, Tuple2[String, Double]) ⇒ Boolean = { (max, p) ⇒ if ((p._2 / max) >= percentage) true else false }

	private val logger = LoggerFactory.getLogger(classOf[CategorizationApplicationObject])
	private val properties = new Properties

	def main(args: Array[String]): Unit = {

		try {
			if (args.length == 0) {
				throw new WrongUsageException
			}

			val propIn = new FileInputStream("../conf/kaw.properties")
			properties.load(propIn)
			val dictionaryLocation = properties.getProperty("pl.gda.pg.eti.kask.kaw.dictionaryLocation")
			val newArticleFile = properties.getProperty("pl.gda.pg.eti.kask.kaw.newArticleOutput")
			val username = properties.getProperty("pl.gda.pg.eti.kask.kaw.userName")

			val folds = properties.getProperty("pl.gda.pg.eti.kask.kaw.folds").toInt
			val randomPerFold = properties.getProperty("pl.gda.pg.eti.kask.kaw.randomPerFold").toInt

			logger.debug("Program start")
			if (args(0).equals("dump")) {
				if (args.length > 1 && args(1).equals("--local")) {
					logger.debug("Uruchamiam zczytywanie artykolow z wikidumps (local)")
					val mainArgs = extractParametersAfterString("--local", args, 2)
					val skipArgs = extractParametersAfterString("--skip", args, 1)
					val maxArgs = extractParametersAfterString("--max", args, 1)
					val startArgs = extractParametersAfterString("--start", args, 1)
					val dropDir = args.contains("--push")
					val skip = if (skipArgs == null) -1 else skipArgs(0).toInt
					val max = if (maxArgs == null) -1 else maxArgs(0).toInt
					val start = if (startArgs == null) -1 else startArgs(0).toInt
					if(mainArgs == null) {
						val dumpIn = properties.getProperty("pl.gda.pg.eti.kask.kaw.localDumpsInput")
						val dumpOut = properties.getProperty("pl.gda.pg.eti.kask.kaw.dumpsOutput")
						new ArticleReader(dumpIn, dumpOut, start, skip, max, dropDir).readAndUpload();
					}
					else {
						new ArticleReader(mainArgs(0), mainArgs(1), start, skip, max, dropDir).readAndUpload();
					}
					return
				}
			}

			if (username == null) {
				throw new InvalidPropertiesException
			}

			val ugi = UserGroupInformation.createRemoteUser(username)

			ugi.doAs(new PrivilegedExceptionAction[Void]() {

				override def run(): Void = {
					logger.debug("Tworze konfiguracje dla klastra")
					val conf = new Configuration

					conf.set("hadoop.job.ugi", username)

					properties.propertyNames().foreach { x =>
						val xStr = x.toString()
						if(xStr.startsWith("hadoop.")) {
							conf.set(xStr.replace("hadoop.", ""), properties.getProperty(xStr))
						}
					}

					var classifyRepetitions = 1
					try {
						classifyRepetitions = properties.getProperty("pl.gda.pg.eti.kask.kaw.classifyRepetitions", "1").toInt
						if(classifyRepetitions < 1) {
							conf.setInt("pl.gda.pg.eti.kask.kaw.classifyRepetitions", 1)
						}
						else {
							conf.setInt("pl.gda.pg.eti.kask.kaw.classifyRepetitions", classifyRepetitions)
						}
					}
					catch {
						case e: Exception =>
							conf.setInt("pl.gda.pg.eti.kask.kaw.classifyRepetitions", 1)
					}
					
					var classifyPrecision = 0.8
					try {
						classifyPrecision = properties.getProperty("pl.gda.pg.eti.kask.kaw.classifierPrecision", "0.8").toDouble
						if(classifyPrecision < 1) {
							conf.setInt("pl.gda.pg.eti.kask.kaw.classifierPrecision", 1)
						}
						else {
							conf.setDouble("pl.gda.pg.eti.kask.kaw.classifierPrecision", classifyPrecision)
						}
					}
					catch {
						case e: Exception =>
							conf.setDouble("pl.gda.pg.eti.kask.kaw.classifierPrecision", 0.8)
					}
					

					val jarFile = properties.getProperty("pl.gda.pg.eti.kask.kaw.jarLocation", "target/kaw-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
					conf.set("pl.gda.pg.eti.kask.kaw.jarLocation", jarFile)

					conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName())
					conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())

					conf.setInt("pl.gda.pg.eti.kask.kaw.folds", folds)
					conf.setInt("pl.gda.pg.eti.kask.kaw.randomPerFold", randomPerFold)
					conf.set("pl.gda.pg.eti.kask.kaw.newArticleOutput", newArticleFile)
					conf.set("pl.gda.pg.eti.kask.kaw.dictionaryLocation", dictionaryLocation)
					conf.set("pl.gda.pg.eti.kask.kaw.kNeighbours", properties.getProperty("pl.gda.pg.eti.kask.kaw.k"))
					
					if (args.length == 2) {
						if (args(1).equals("--auto")) {
							if (args(0).equals("classify")) {
								val hdfs = FileSystem.get(conf)
								// word count on new article
								if(!hdfs.exists(new Path(properties.getProperty("pl.gda.pg.eti.kask.kaw.newArticleOutput"))))
									queueTask(conf, new WordCountTask, "pl.gda.pg.eti.kask.kaw.newArticleInput", "pl.gda.pg.eti.kask.kaw.newArticleOutput")

								// word count on other articles
								if(!hdfs.exists(new Path(properties.getProperty("pl.gda.pg.eti.kask.kaw.wordCountOutput"))))
									queueTask(conf, new WordCountTask, "pl.gda.pg.eti.kask.kaw.wordCountInput", "pl.gda.pg.eti.kask.kaw.wordCountOutput")

								// dispose dictionary
								CategorizationApplicationObject.logger.debug("Usuwanie slownika odmian")
								TokenizerMapper.disposeDictionary
								CategorizationApplicationObject.logger.debug("Usunieto, zwalnianie pamieci przez garbage collector..")

								// similarity on all article
								val cfOutputPath = new Path(properties.getProperty("pl.gda.pg.eti.kask.kaw.classifierOutput"))
								if(hdfs.exists(cfOutputPath))
									hdfs.delete(cfOutputPath, true)
								queueTask(conf, new NoMatrixuSimilarityTask, "pl.gda.pg.eti.kask.kaw.classifierInput", "pl.gda.pg.eti.kask.kaw.classifierOutput")

								// best results
								val bIn = properties.getProperty("pl.gda.pg.eti.kask.kaw.classifierOutput")
								val bK = properties.getProperty("pl.gda.pg.eti.kask.kaw.k")
								println("Wyniki klasyfikacji:")
								if(classifyRepetitions > 1) {
									// TYLKO w celach testowych, nie usprawnia klasyfikatora
									// ONLY for speed testing purpouses, does not improve classifier
									for (i ← 0 to classifyRepetitions - 1) {
										new CosineSimilarityIndexCounter().getBestCategories(conf, bIn, bK.toInt, true)(getStrategy(conf))
									}
								}

								val bestRes = new CosineSimilarityIndexCounter().getBestCategories(conf, bIn, bK.toInt, true)(getStrategy(conf))
								CategorizationApplicationObject.logger.debug("Uzyskano wyniki klasyfikacji")
								bestRes.foreach { x ⇒ println(x) }
								return null
							} else if (args(0).equals("crossvalidate")) {
								val hdfs = FileSystem.get(conf)
								// word count on articles
								if(!hdfs.exists(new Path(properties.getProperty("pl.gda.pg.eti.kask.kaw.wordCountOutput"))))
									queueTask(conf, new WordCountTask, "pl.gda.pg.eti.kask.kaw.wordCountInput", "pl.gda.pg.eti.kask.kaw.wordCountOutput")

								// dispose dictionary
								CategorizationApplicationObject.logger.debug("Usuwanie slownika odmian")
								TokenizerMapper.disposeDictionary
								CategorizationApplicationObject.logger.debug("Usunieto, zwalnianie pamieci przez garbage collector..")

								// folding on articles
								val foldOutputPath = new Path(properties.getProperty("pl.gda.pg.eti.kask.kaw.foldingOutput"))
								if(hdfs.exists(foldOutputPath))
									hdfs.delete(foldOutputPath, true)
								queueTask(conf, new FoldingClusterTask, "pl.gda.pg.eti.kask.kaw.foldingInput", "pl.gda.pg.eti.kask.kaw.foldingOutput")

								// cross validate articles
								val cvOutputPath = new Path(properties.getProperty("pl.gda.pg.eti.kask.kaw.crossvalidationOutput"))
								if(hdfs.exists(cvOutputPath))
									hdfs.delete(cvOutputPath, true)
								queueTask(conf, new CrossValidationTask, "pl.gda.pg.eti.kask.kaw.crossvalidationInput", "pl.gda.pg.eti.kask.kaw.crossvalidationOutput")

								// scores
								val cvsOutputPath = new Path(properties.getProperty("pl.gda.pg.eti.kask.kaw.crossvalidationScoresOutput"))
								if(hdfs.exists(cvsOutputPath))
									hdfs.delete(cvsOutputPath, true)
								queueTask(conf, new CrossValidationResultsTask, "pl.gda.pg.eti.kask.kaw.crossvalidationScoresInput", "pl.gda.pg.eti.kask.kaw.crossvalidationScoresOutput")

								// scores
								val cvaOutputPath = new Path(properties.getProperty("pl.gda.pg.eti.kask.kaw.crossvalidationAverageScoreOutput"))
								if(hdfs.exists(cvaOutputPath))
									hdfs.delete(cvaOutputPath, true)
								queueTask(conf, new CrossValidationAverageCounterTask, "pl.gda.pg.eti.kask.kaw.crossvalidationAverageScoreInput", "pl.gda.pg.eti.kask.kaw.crossvalidationAverageScoreOutput")

								// read results
								printDirContents(hdfs, cvaOutputPath)
							}
						}
					}

					if (args(0).equals("bestCategories")) {
						var bIn = ""
						var bK = ""
						val bArgs = extractParametersAfterString("bestCategories", args, 2)
						if (bArgs!=null) {
							bIn = bArgs(0)
							bK = bArgs(1)
							if(bIn == null || bK == null)
								throw new WrongUsageException()
						} else {
							bIn = properties.getProperty("pl.gda.pg.eti.kask.kaw.classifierOutput")
							bK = properties.getProperty("pl.gda.pg.eti.kask.kaw.k")
							if(bIn == null || bK == null)
								throw new InvalidPropertiesException()
						}
						if(classifyRepetitions > 1) {
							// TYLKO w celach testowych, nie usprawnia klasyfikatora
							// ONLY for speed testing purpouses, does not improve classifier
							for (i ← 0 to classifyRepetitions - 1) {
								new CosineSimilarityIndexCounter().getBestCategories(conf, bIn, bK.toInt, true)(getStrategy(conf))
							}
						}
						val bestCats = new CosineSimilarityIndexCounter().getBestCategories(conf, bIn, bK.toInt, true)(getStrategy(conf))
						CategorizationApplicationObject.logger.debug("Uzyskano wyniki klasyfikacji")
						bestCats.foreach { x ⇒ println(x) }
						return null
					}

					if (args(0).equals("dump")) {
						logger.debug("Pobieram system plikow z konfiguracji")
						val hdfs = FileSystem.get(conf)
						logger.debug("Uruchamiam zczytywanie artykolow z wikidumps")

						val mainArgs = extractParametersAfterString("dump", args, 2)
						val skipArgs = extractParametersAfterString("--skip", args, 1)
						val maxArgs = extractParametersAfterString("--max", args, 1)
						val startArgs = extractParametersAfterString("--start", args, 1)
						val dropDir = args.contains("--push")
						val skip = if (skipArgs == null) -1 else skipArgs(0).toInt
						val max = if (maxArgs == null) -1 else maxArgs(0).toInt
						val start = if (startArgs == null) -1 else startArgs(0).toInt
						if(mainArgs == null) {
							val dumpIn = properties.getProperty("pl.gda.pg.eti.kask.kaw.localDumpsInput")
							val dumpOut = properties.getProperty("pl.gda.pg.eti.kask.kaw.dumpsOutput")
							new DistributedArticleReader(dumpIn, dumpOut, hdfs, start, skip, max, dropDir).readAndUpload();
						}
						else {
							new DistributedArticleReader(mainArgs(0), mainArgs(1), hdfs, start, skip, max, dropDir).readAndUpload();
						}
						logger.debug("Zamykam system plikow")
						hdfs.close
						return null
					}

					if(!args.contains("--auto")) {
						attachTask("wordcount", conf, args, new WordCountTask, "pl.gda.pg.eti.kask.kaw.wordCountInput", "pl.gda.pg.eti.kask.kaw.wordCountOutput")
						attachTask("classify", conf, args, new NoMatrixuSimilarityTask, "pl.gda.pg.eti.kask.kaw.classifierInput", "pl.gda.pg.eti.kask.kaw.classifierOutput")
						attachTask("fold", conf, args, new FoldingClusterTask, "pl.gda.pg.eti.kask.kaw.foldingInput", "pl.gda.pg.eti.kask.kaw.foldingOutput")
						attachTask("crossvalidate", conf, args, new CrossValidationTask, "pl.gda.pg.eti.kask.kaw.crossvalidationInput", "pl.gda.pg.eti.kask.kaw.crossvalidationOutput")
						attachTask("cvaverage", conf, args, new CrossValidationAverageCounterTask, "pl.gda.pg.eti.kask.kaw.crossvalidationAverageScoreInput", "pl.gda.pg.eti.kask.kaw.crossvalidationAverageScoreOutput")
						attachTask("cvscores", conf, args, new CrossValidationResultsTask, "pl.gda.pg.eti.kask.kaw.crossvalidationScoresInput", "pl.gda.pg.eti.kask.kaw.crossvalidationScoresOutput")
					}

					TokenizerMapper.disposeDictionary
					return null
				}
			})
		} catch {
			case ipe: InvalidPropertiesException ⇒ println(ipe.getMessage)
			case wue: WrongUsageException ⇒
				println(wue.getMessage); printManual
			case e: Exception ⇒ e.printStackTrace
		}
	}

	private def queueTask(conf: Configuration, task: ClusterTask, inProperty: String, outProperty: String) = {
		val inputDir = properties.getProperty(inProperty)
		val outputDir = properties.getProperty(outProperty)
		task.runTask(conf, Array[String](inputDir, outputDir))
	}

	private def extractParametersAfterString(after: String, args: Array[String], howMany: Int): Array[String] = {
		if(args.contains(after)) {
			val ind = args.indexOf(after)
			var i = 0
			val list = MutableList[String]()
			for(i <- (ind + 1) to (ind + howMany)) {
				list += args(i)
			}
			list.toArray
		}
		else {
			null
		}
	}

	private def attachTask(cmd: String, conf: Configuration, args: Array[String], task: ClusterTask, inProperty: String, outProperty: String) = {
		if (args(0).equals(cmd)) {
			if (args.length < 3) {
				val inputDir = properties.getProperty(inProperty)
				val outputDir = properties.getProperty(outProperty)
				task.runTask(conf, Array[String](inputDir, outputDir))
			}
			else {
				task.runTask(conf, extractParametersAfterString(cmd, args, 2))
			}
		}
		else {
			1
		}
	}

	private def printManual {
		val sc = new Scanner(classOf[CategorizationApplicationObject].getResourceAsStream("/manual"), "UTF-8")
		while (sc.hasNextLine()) {
			println(sc.nextLine())
		}
	}

	private def printDirContents(hdfs: FileSystem, dir: Path) {
		val stat = hdfs.listStatus(dir)
		stat.foreach { stat =>
			val file = hdfs.open(stat.getPath)
			val scanner = new Scanner(file, "UTF-8")
			while(scanner.hasNextLine) {
				val line = scanner.nextLine
				if(line != null && !line.isEmpty)
					println(line)
			}
		}
	}
}
