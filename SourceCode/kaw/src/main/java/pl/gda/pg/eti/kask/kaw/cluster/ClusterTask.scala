package pl.gda.pg.eti.kask.kaw.cluster

import org.apache.hadoop.conf.Configuration

trait ClusterTask {
	def runTask(conf: Configuration, args: Array[String]): Int
}