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

class CategorizationApplicationObject {
}

object CategorizationApplicationObject {

	private val USER_NAME = "143351sw"
	private val logger = LoggerFactory.getLogger(classOf[CategorizationApplicationObject])

	def main(args: Array[String]): Unit = {
		logger.debug("Program start")
		if (args(0).equals("--dump")) {
			if (args(1).equals("--local")) {
				logger.debug("Uruchamiam zczytywanie artykolow z wikidumps (local)")
				new ArticleReader(args(2), args(3)).readAndUpload();
				return
			}
		}

		val ugi = UserGroupInformation.createRemoteUser(USER_NAME)
		try {
			ugi.doAs(new PrivilegedExceptionAction[Void]() {

				override def run(): Void = {
					logger.debug("Tworze konfiguracje dla klastra")
					val conf = new Configuration

					conf.set("hadoop.job.ugi", USER_NAME)
					// conf.set("mapred.job.tracker", "des01.eti.pg.gda.pl:54311")
					// conf.set("fs.defaultFS", "hdfs://des01.eti.pg.gda.pl:54310")
					// conf.set("mapreduce.framework.name", "yarn");
					// conf.set("yarn.resourcemanager.address", "des01.eti.pg.gda.pl:8032");
					// conf.set("yarn.resourcemanager.scheduler.address", "des01.eti.pg.gda.pl:8030")

					conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName())
					conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())

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
					}
					else {
						val task = new WordCountTask
						System.exit(task.runTask(conf, b.toArray))
					}

					return null
				}
			})
		}
		catch {
			case e: Exception ⇒ e.printStackTrace
		}
	}
}
