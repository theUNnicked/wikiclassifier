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

class CategorizationApplicationObject {
}

object CategorizationApplicationObject {

	private val logger = LoggerFactory.getLogger(classOf[CategorizationApplicationObject])
	private val properties = new Properties
	private var dictionaryLocation = ""
	private var newArticleFile = ""
	private var username = ""

	def getNewArticleFileName = { newArticleFile }
	def getUsername = { username }
	def getDictionaryLocation = { dictionaryLocation }

	def main(args: Array[String]): Unit = {

		properties.load(classOf[CategorizationApplicationObject].getResourceAsStream("/application.properties"));
		dictionaryLocation = properties.getProperty("dictionaryLocation")
		newArticleFile = properties.getProperty("newArticleOutputFile")
		username = properties.getProperty("userName")

		val trainingSet = properties.getProperty("trainingSet").toInt
		val testingSet = properties.getProperty("testingSet").toInt

		logger.debug("Program start")
		if (args(0).equals("--dump")) {
			if (args(1).equals("--local")) {
				logger.debug("Uruchamiam zczytywanie artykolow z wikidumps (local)")
				new ArticleReader(args(2), args(3)).readAndUpload();
				return
			}
		}

		val ugi = UserGroupInformation.createRemoteUser(getUsername)
		try {
			ugi.doAs(new PrivilegedExceptionAction[Void]() {

				override def run(): Void = {
					logger.debug("Tworze konfiguracje dla klastra")
					val conf = new Configuration

					conf.set("hadoop.job.ugi", getUsername)
					// conf.set("mapred.job.tracker", "des01.eti.pg.gda.pl:54311")
					// conf.set("fs.defaultFS", "hdfs://des01.eti.pg.gda.pl:54310")
					// conf.set("mapreduce.framework.name", "yarn");
					// conf.set("yarn.resourcemanager.address", "des01.eti.pg.gda.pl:8032");
					// conf.set("yarn.resourcemanager.scheduler.address", "des01.eti.pg.gda.pl:8030")

					conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName())
					conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())

					conf.setInt("pl.gda.pg.eti.kask.kaw.testingSet", testingSet)
					conf.setInt("pl.gda.pg.eti.kask.kaw.allSet", trainingSet + testingSet)
					conf.set("pl.gda.pg.eti.kask.kaw.newArticleFile", getNewArticleFileName)
					conf.set("pl.gda.pg.eti.kask.kaw.dictionaryLocation", getDictionaryLocation)

					if (args(0).equals("--best")) {
						println("Wybrane kategorie dla artykulu Dota_2:")
						val strategyBest70Percent: (Double, Tuple2[String, Double]) => Boolean = { (max, p) => if (p._2 > max * 0.7) true else false }
						if (args(1).equals("--70p")) {
							new CosineSimilarityIndexCounter().getBestCategories(conf, args(2), args(3).toInt, true)(strategyBest70Percent).foreach { x => println(x) }
						} else {
							new CosineSimilarityIndexCounter().getBestCategories(conf, args(1), args(2).toInt, true)(strategyBest70Percent).foreach { x => println(x) }
						}
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

					val b = args.toBuffer
					b.remove(0)
					if (args(0).equals("--wordcount")) {
						val task = new WordCountTask
						System.exit(task.runTask(conf, b.toArray))
					} else if (args(0).equals("--classify")) {
						val task = new NoMatrixuSimilarityTask
						System.exit(task.runTask(conf, b.toArray))
					}

					return null
				}
			})
		} catch {
			case e: Exception ⇒ e.printStackTrace
		}
	}
}
