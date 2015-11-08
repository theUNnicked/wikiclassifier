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

class WrongUsageException extends Exception("Wrong usage, check your parameters and try again");
class InvalidPropertiesException extends Exception("Wrong parameters, check parameter file [kaw.properties]");

class CategorizationApplicationObject {}

object CategorizationApplicationObject {

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
				if (args(1).equals("--local")) {
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
					conf.set("pl.gda.pg.eti.kask.kaw.newArticleFile", newArticleFile)
					conf.set("pl.gda.pg.eti.kask.kaw.dictionaryLocation", dictionaryLocation)
					val strategyBest70Percent: (Double, Tuple2[String, Double]) ⇒ Boolean = { (max, p) ⇒ if (p._2 > max * 0.7) true else false }
					if (args(0).equals("--best")) {
						if (args(1).equals("--70p")) {
							if (args.length > 4) {
								throw new WrongUsageException
							}
							new CosineSimilarityIndexCounter().getBestCategories(conf, args(2), args(3).toInt, true)(strategyBest70Percent).foreach { x ⇒ println(x) }
						}
						else {
							if (args.length > 3) {
								throw new WrongUsageException
							}
							new CosineSimilarityIndexCounter().getBestCategories(conf, args(1), args(2).toInt, true)(strategyBest70Percent).foreach { x ⇒ println(x) }
						}
						return null
					}

					if (args(0).equals("--crosvalresults")) {
						val hdfs = FileSystem.get(conf)
						if (args(1).equals("--70p")) {
							if (args.length > 4) {
								throw new WrongUsageException
							}
							new CrossValidation().validate(args(2), hdfs, args(3).toInt)(strategyBest70Percent).foreach { case (name, res) ⇒ println("Article name: " + name + ", Prediction: " + res.toString) }
						}
						else {
							if (args.length > 3) {
								throw new WrongUsageException
							}
							new CrossValidation().validate(args(1), hdfs, args(2).toInt)(strategyBest70Percent).foreach { case (name, res) ⇒ println("Article name: " + name + ", Prediction: " + res.toString) }
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
						new DistributedArticleReader(args(1), args(2), hdfs).readAndUpload();
						logger.debug("Zamykam system plikow")
						hdfs.close
						System.exit(0)
						return null
					}

					if (args(0).equals("--wordcount")) {
						val task = new WordCountTask
						if (args.length < 3) {
							val inputDir = properties.getProperty("pl.gda.pg.eti.kask.kaw.wordCountInput")
							val outputDir = properties.getProperty("pl.gda.pg.eti.kask.kaw.wordCountOutput")
							System.exit(task.runTask(conf, Array[String](inputDir, outputDir)))
						}
						System.exit(task.runTask(conf, args.takeRight(2)))
					}
					else if (args(0).equals("--classify")) {
						val task = new NoMatrixuSimilarityTask
						if (args.length < 3) {
							val inputDir = properties.getProperty("pl.gda.pg.eti.kask.kaw.classifierInput")
							val outputDir = properties.getProperty("pl.gda.pg.eti.kask.kaw.classifierOutput")
							System.exit(task.runTask(conf, Array[String](inputDir, outputDir)))
						}
						System.exit(task.runTask(conf, args.takeRight(2)))
					}
					else if (args(0).equals("--fold")) {
						val task = new FoldingClusterTask
						if (args.length < 3) {
							val inputDir = properties.getProperty("pl.gda.pg.eti.kask.kaw.foldingInput")
							val outputDir = properties.getProperty("pl.gda.pg.eti.kask.kaw.foldingOutput")
							System.exit(task.runTask(conf, Array[String](inputDir, outputDir)))
						}
						System.exit(task.runTask(conf, args.takeRight(2)))
					}
					else if (args(0).equals("--crossvalidation")) {
						val task = new CrossValidationTask
						if (args.length < 3) {
							val inputDir = properties.getProperty("pl.gda.pg.eti.kask.kaw.crossvalidationInput")
							val outputDir = properties.getProperty("pl.gda.pg.eti.kask.kaw.crossvalidationOutput")
							System.exit(task.runTask(conf, Array[String](inputDir, outputDir)))
						}
						System.exit(task.runTask(conf, args.takeRight(2)))
					}

					return null
				}
			})
		}
		catch {
			case ipe: InvalidPropertiesException ⇒ println(ipe.getMessage)
			case wue: WrongUsageException ⇒
				println(wue.getMessage); printManual
			case e: Exception ⇒ e.printStackTrace
		}
	}

	private def printManual {
		val sc = new Scanner(classOf[CategorizationApplicationObject].getResourceAsStream("/manual"), "UTF-8")
		while (sc.hasNextLine()) {
			println(sc.nextLine())
		}
	}
}