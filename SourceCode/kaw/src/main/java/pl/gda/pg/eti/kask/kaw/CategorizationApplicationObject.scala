package pl.gda.pg.eti.kask.kaw

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration
import java.security.PrivilegedExceptionAction
import pl.gda.pg.eti.kask.kaw.dump.ArticleReader
import pl.gda.pg.eti.kask.kaw.dump.DistributedArticleReader
import org.apache.hadoop.fs.FileSystem
//import org.slf4j.Logger

object CategorizationApplicationObject {

	private val USER_NAME = "143351sw"
	//	private val logger = Logger.getLogger(CategorizationApplicationObject.getClass)

	def main(args: Array[String]): Unit = {

		if (args(0).equals("--dump")) {
			if (args(1).equals("--local")) {
				new ArticleReader(args(2), args(3)).readAndUpload();
				return
			}
		}

		val ugi = UserGroupInformation.createRemoteUser(USER_NAME)
		try {
			ugi.doAs(new PrivilegedExceptionAction[Void]() {

				override def run(): Void = {
					val conf = new Configuration
					conf.set("hadoop.job.ugi", USER_NAME);
					conf.set("mapred.job.tracker", "des01.eti.pg.gda.pl:54311")
					conf.set("fs.defaultFS", "hdfs://des01.eti.pg.gda.pl:54310")
					conf.set("mapreduce.framework.name", "yarn");
					conf.set("yarn.resourcemanager.address", "des01.eti.pg.gda.pl:8032");
					conf.set("yarn.resourcemanager.scheduler.address", "des01.eti.pg.gda.pl:8030")

					if (args(0).equals("--dump")) {
						val hdfs = FileSystem.get(conf)
						new DistributedArticleReader(args(1), args(2), hdfs).readAndUpload();
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
