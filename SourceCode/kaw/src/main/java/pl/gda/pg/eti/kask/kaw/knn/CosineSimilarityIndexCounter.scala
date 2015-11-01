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

class CosineSimilarityIndexCounter(private val threshold: Double) {

	@deprecated(message = "Uzywaj konstruktora z parametrem")
	def this() = this(0)

	def findBestCategories(articles: Map[Double, List[String]]): List[String] = {

		null
	}

	def extractKBestArticles(conf: Configuration, outputDir: String, k: Int) {
		val hdfs = FileSystem.get(conf)
		val files = hdfs.listStatus(new Path(outputDir));
		var i = 0
		
		var best = Array.fill(k){""}
		var min = 0.0
		
		while (i < files.length) {
			val file = hdfs.open(files(i).getPath)
			val bin = new BufferedReader(new InputStreamReader(file))
			Stream.continually(bin.readLine).takeWhile(_ != null).foreach { line => 
				val parts = line.split("\t")
				// TODO: wybieranie k najlepszych
			}

			bin.close
			file.close
			i = i + 1
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