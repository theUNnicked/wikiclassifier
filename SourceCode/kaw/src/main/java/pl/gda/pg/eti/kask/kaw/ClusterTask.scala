package pl.gda.pg.eti.kask.kaw

trait ClusterTask {
	def runTask(args: Array[String]): Int
}