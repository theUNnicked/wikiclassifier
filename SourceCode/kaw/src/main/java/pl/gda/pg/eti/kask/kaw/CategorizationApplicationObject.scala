package pl.gda.pg.eti.kask.kaw

object CategorizationApplicationObject {
	def main(args: Array[String]): Unit = {
		val task = new WordCountTask
		System.exit(task.runTask(args));
	}
}
