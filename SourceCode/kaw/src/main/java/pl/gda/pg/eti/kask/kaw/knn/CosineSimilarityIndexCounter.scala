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
import java.util.Scanner

class CosineSimilarityIndexCounter() {

	def getBestCategories(conf: Configuration, outputDir: String, k: Int, precise: Boolean)(thresholdingStrategy: (Double, Tuple2[String, Double]) ⇒ Boolean): List[String] = {
		val extractor = new NearestNeighboursCategoryExtractor()
		val extractCategoriesWithSimilarityFromString = { x: String ⇒
			if (!x.isEmpty) {
				val split = x.split("\t")
				(split(1).toDouble, split.takeRight(split.length - 2).toList)
			}
			null
		}
		extractor.extractCategories(extractKBestArticlesAsArray(conf, outputDir, k), extractCategoriesWithSimilarityFromString, thresholdingStrategy)
	}

	private def extractKBestArticlesAsArray(conf: Configuration, outputDir: String, k: Int): Array[String] = {
		val hdfs = FileSystem.get(conf)
		val files = hdfs.listStatus(new Path(outputDir))
		val extractor = new KNearestNeighboursExtract(k)
		var best: Array[String] = null
		for (i ← 0 to files.length - 1) {
			if (!files(i).getPath.toString.contains(".crc")) {
				val file = hdfs.open(files(i).getPath)
				val scanner = new Scanner(file, "UTF-8")
				val hasNext = { (scan: AnyRef, lineNo: Int) ⇒ scanner.asInstanceOf[Scanner].hasNext() }
				val getNext = { (scan: AnyRef, lineNo: Int) ⇒ scanner.asInstanceOf[Scanner].nextLine() }
				val extractSimilarity = { (line: String) ⇒ line.split("\t")(1).toDouble }
				extractor.extractKNearestNeighbours(scanner, hasNext, getNext, extractSimilarity)
				while (scanner.hasNextLine()) {
					val line = scanner.nextLine()
					if (line != null && !line.isEmpty()) {

					}
				}
				file.close()
				scanner.close()
			}
		}
		best
	}

	@deprecated(message = "Uzywaj getBestCategories")
	def countSimilarity(articlesIds: Iterable[Int], wordsWithCount: Iterable[Word], context: Reducer[IntWritable, Word, Text, IntWritable]#Context) {
		val categoryFinder = new CategoryFinder
		var iterator = articlesIds.iterator
		var categoriesMap = scala.collection.mutable.Map[String, Int]()
		while (iterator.hasNext) {
			// TODO zmiana funkcji z tymczasowej na właściwą
			val categories = categoryFinder.findCategories(iterator.next)
			for (x ← 0 to categories.length - 1) {
				if (!categoriesMap.contains(categories(x))) {
					categoriesMap += (categories(x) -> 1)
				}
				else {
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