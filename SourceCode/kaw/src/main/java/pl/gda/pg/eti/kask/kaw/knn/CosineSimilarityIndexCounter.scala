package pl.gda.pg.eti.kask.kaw.knn

import pl.gda.pg.eti.kask.kaw.extract.Word
import pl.gda.pg.eti.kask.kaw.extract.CategoryFinder
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.InputStreamReader
import java.io.BufferedReader

class CosineSimilarityIndexCounter() {

	def findBestCategories(articles: Map[Double, Array[String]], precise: Boolean)(thresholdingStrategy: (Double, Tuple2[String, Double]) => Boolean): List[String] = {
		var map = scala.collection.mutable.Map[String, Double]()
		articles.foreach{ tp =>
			tp._2.foreach { cat => 
				if(map.contains(cat)) {
					if(precise) {
						map(cat) += tp._1
					}
					else {
						map(cat) += 1
					}
				}
				else {
					if(precise) {
						map += cat -> tp._1
					}
					else {
						map += cat -> 1
					}
				}
			}
		}
		
		val best = map.toSeq.sortBy(-_._2)
		val max = best(0)._2
		best.takeWhile { s => thresholdingStrategy(max, s) }.foldLeft[List[String]](List[String]()) { (list, e) => list :+ e._1 }
	}
	
	def getBestCategories(conf: Configuration, outputDir: String, k: Int, precise: Boolean)(thresholdingStrategy: (Double, Tuple2[String, Double]) => Boolean): List[String] = {
		findBestCategories(extractKBestArticles(conf, outputDir, k), precise)(thresholdingStrategy)
	}
	
	def extractKBestArticles(conf: Configuration, outputDir: String, k: Int): Map[Double, Array[String]] = {
		var map = Map[Double, Array[String]]()
		val array = extractKBestArticlesAsArray(conf, outputDir, k)
		array.foreach { x =>  
			if(!x.isEmpty) {
				val split = x.split("\t")
				map += split(1).toDouble -> split.takeRight(split.length - 2)
			}
		}
		map
	}

	private def extractKBestArticlesAsArray(conf: Configuration, outputDir: String, k: Int): Array[String] = {
		val hdfs = FileSystem.get(conf)
		val files = hdfs.listStatus(new Path(outputDir));
		var i = 0

		var best = Array.fill(k) { "" }
		var min = (0, 0.0)

		while (i < files.length) {
			if(!files(i).getPath.toString.contains(".crc")) {
				val file = hdfs.open(files(i).getPath)
				val bin = new BufferedReader(new InputStreamReader(file))
				Stream.continually(bin.readLine).takeWhile(_ != null).foreach { line =>
					if(line != null && !line.isEmpty()) {
						if (line.split("\t")(1).toDouble > min._2) {
							best(min._1) = line
							min = findMinimumWithIndex(best)
						}
					}
				}
				bin.close
				file.close
			}
			i = i + 1
		}
		
		best
	}

	private def findMinimumWithIndex(best: Array[String]): Tuple2[Int, Double] = {
		best.zipWithIndex.foldLeft[Tuple2[Int, Double]]((0, Integer.MAX_VALUE)) { (last, x) =>
			if(!x._1.isEmpty) {
				val sim = x._1.split("\t")(1).toDouble
				if (sim < last._2) (x._2, sim) else last
			}
			last
		}
	}

	@deprecated(message = "Uzywaj findBestCategories")
	def countSimilarity(articlesIds: Iterable[Int], wordsWithCount: Iterable[Word], context: Reducer[IntWritable, Word, Text, IntWritable]#Context) {
		val categoryFinder = new CategoryFinder
		var iterator = articlesIds.iterator
		var categoriesMap = scala.collection.mutable.Map[String, Int]()
		while (iterator.hasNext) {
			// TODO zmiana funkcji z tymczasowej na właściwą
			val categories = categoryFinder.findCategories(iterator.next)
			for (x <- 0 to categories.length - 1) {
				if (!categoriesMap.contains(categories(x))) {
					categoriesMap += (categories(x) -> 1)
				} else {
					categoriesMap(categories(x)) += 1
				}
			}
		}
		val predictedClasses = categoriesMap.toSeq.sortBy(-_._2).take(3)
		println(predictedClasses)

		// TODO Zapisanie do contextu
		//context.write(new Text(name), new IntWritable(count))
	}
}