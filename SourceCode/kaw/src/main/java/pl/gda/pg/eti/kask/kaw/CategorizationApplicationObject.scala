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

class WrongUsageException extends Exception("Wrong usage, check your parameters and try again");
class InvalidPropertiesException extends Exception("Wrong parameters, check parameter file [kaw.properties]");

class CategorizationApplicationObject {}

object CategorizationApplicationObject {

	val strategyBest70Percent: (Double, Tuple2[String, Double]) ⇒ Boolean = { (max, p) ⇒ if (p._2 > max * 0.9) true else false }
	
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
			if (args(0).equals("--dump")) {
				if (args.length > 1 && args(1).equals("--local")) {
					if (args.length > 4) {
						throw new WrongUsageException
					}
					logger.debug("Uruchamiam zczytywanie artykolow z wikidumps (local)")
					new ArticleReader(args(2), args(3)).readAndUpload();
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
					val jobtracker = properties.getProperty("hadoop.mapred.job.tracker")
					if (jobtracker != null) {
						conf.set("mapred.job.tracker", jobtracker)
					}
					val fs = properties.getProperty("hadoop.fs.defaultFS")
					if (fs != null) {
						conf.set("fs.defaultFS", fs)
					}
					val mrFramework = properties.getProperty("hadoop.mapreduce.framework.name")
					if (mrFramework != null) {
						conf.set("mapreduce.framework.name", mrFramework);
					}
					val rmAddress = properties.getProperty("hadoop.yarn.resourcemanager.address")
					if (rmAddress != null) {
						conf.set("yarn.resourcemanager.address", rmAddress);
					}
					val rmScheduler = properties.getProperty("hadoop.yarn.resourcemanager.scheduler.address")
					if (rmScheduler != null) {
						conf.set("yarn.resourcemanager.scheduler.address", rmScheduler)
					}

					conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName())
					conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())

					conf.setInt("pl.gda.pg.eti.kask.kaw.folds", folds)
					conf.setInt("pl.gda.pg.eti.kask.kaw.randomPerFold", randomPerFold)
					conf.set("pl.gda.pg.eti.kask.kaw.newArticleOutput", newArticleFile)
					conf.set("pl.gda.pg.eti.kask.kaw.dictionaryLocation", dictionaryLocation)
					conf.set("pl.gda.pg.eti.kask.kaw.kNeighbours", properties.getProperty("pl.gda.pg.eti.kask.kaw.k"))

					if (args(0).equals("--auto")) {
						if (args(1).equals("--classify")) {
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
							new CosineSimilarityIndexCounter().getBestCategories(conf, bIn, bK.toInt, true)(strategyBest70Percent).foreach { x ⇒ println(x) }
							return null
						} else if (args(1).equals("--crossvalidation")) {
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

					if (args(0).equals("--best")) {
						var bIn = ""
						var bK = ""
						if (args.length == 4) {
							bIn = args(2)
							bK = args(3)
						} else if (args.length == 3) {
							bIn = args(1)
							bK = args(2)
						} else {
							bIn = properties.getProperty("pl.gda.pg.eti.kask.kaw.classifierOutput")
							bK = properties.getProperty("pl.gda.pg.eti.kask.kaw.k")
						}
						if (args.length > 2) {
							if (args(1).equals("--90p")) {
								if (args.length > 4) {
									throw new WrongUsageException
								}
								new CosineSimilarityIndexCounter().getBestCategories(conf, bIn, bK.toInt, true)(strategyBest70Percent).foreach { x ⇒ println(x) }
							}
						} else {
							if (args.length > 3) {
								throw new WrongUsageException
							}
							new CosineSimilarityIndexCounter().getBestCategories(conf, bIn, bK.toInt, true)(strategyBest70Percent).foreach { x ⇒ println(x) }
						}
						return null
					}

					if (args.length > 3) {
						throw new WrongUsageException
					}

					if (args(0).equals("--dump")) {
						logger.debug("Pobieram system plikow z konfiguracji")
						val hdfs = FileSystem.get(conf)
						logger.debug("Uruchamiam zczytywanie artykolow z wikidumps")
						if (args.length < 3) {
							val dumpIn = properties.getProperty("pl.gda.pg.eti.kask.kaw.localDumpsInput")
							val dumpOut = properties.getProperty("pl.gda.pg.eti.kask.kaw.dumpsOutput")
							new DistributedArticleReader(dumpIn, dumpOut, hdfs).readAndUpload();
						} else {
							new DistributedArticleReader(args(1), args(2), hdfs).readAndUpload();
						}
						logger.debug("Zamykam system plikow")
						hdfs.close
						System.exit(0)
						return null
					}
					
					attachTask("--wordcount", conf, args, new WordCountTask, "pl.gda.pg.eti.kask.kaw.wordCountInput", "pl.gda.pg.eti.kask.kaw.wordCountOutput")
					attachTask("--classify", conf, args, new NoMatrixuSimilarityTask, "pl.gda.pg.eti.kask.kaw.classifierInput", "pl.gda.pg.eti.kask.kaw.classifierOutput")
					attachTask("--fold", conf, args, new FoldingClusterTask, "pl.gda.pg.eti.kask.kaw.foldingInput", "pl.gda.pg.eti.kask.kaw.foldingOutput")
					attachTask("--crossvalidation", conf, args, new CrossValidationTask, "pl.gda.pg.eti.kask.kaw.crossvalidationInput", "pl.gda.pg.eti.kask.kaw.crossvalidationOutput")
					attachTask("--cvaverage", conf, args, new CrossValidationAverageCounterTask, "pl.gda.pg.eti.kask.kaw.crossvalidationAverageScoreInput", "pl.gda.pg.eti.kask.kaw.crossvalidationAverageScoreOutput")
					attachTask("--cvscores", conf, args, new CrossValidationResultsTask, "pl.gda.pg.eti.kask.kaw.crossvalidationScoresInput", "pl.gda.pg.eti.kask.kaw.crossvalidationScoresOutput")

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

	private def attachTask(cmd: String, conf: Configuration, args: Array[String], task: ClusterTask, inProperty: String, outProperty: String) = {
		if (args(0).equals(cmd)) {
			if (args.length < 3) {
				val inputDir = properties.getProperty(inProperty)
				val outputDir = properties.getProperty(outProperty)
				task.runTask(conf, Array[String](inputDir, outputDir))
			}
			else {
				task.runTask(conf, args.takeRight(2))
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
