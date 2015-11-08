package pl.gda.pg.eti.kask.kaw.grade

import scala.collection.mutable.ArrayBuffer
import pl.gda.pg.eti.kask.kaw.extract.Word
import pl.gda.pg.eti.kask.kaw.extract.CategoryFinder
import pl.gda.pg.eti.kask.kaw.knn.KnnClassifier
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.Scanner
import scala.collection.mutable.MutableList
import pl.gda.pg.eti.kask.kaw.knn.KNearestNeighboursExtract
import pl.gda.pg.eti.kask.kaw.knn.NearestNeighboursCategoryExtractor
//import.pl.gda.pg.eti.kask.kaw.knn.CosineSimilarityIndexCounter

class CrossValidation {

	def validate(outputDir: String, hdfs: FileSystem, k: Int)(thresholdingStrategy: (Double, Tuple2[String, Double]) ⇒ Boolean) = {
		val resultsMap = scala.collection.mutable.Map[String, Double]()
		val files = hdfs.listStatus(new Path(outputDir));
		var i = 0

		for (i ← 0 to files.length - 1) {
			if (!files(i).getPath.toString.contains(".crc")) {
				val file = hdfs.open(files(i).getPath)
				val scanner = new Scanner(file, "UTF-8");
				while (scanner.hasNextLine()) {
					val line = scanner.nextLine();
					val articleName = line.substring(0, line.indexOf('['))
					val expectedCategories = extractExpectedCategories(line)
					val articles = extractKBestArticles(line, k)
					val predictedCategories = extractPredictedCategories(articles)(thresholdingStrategy)
					if (resultsMap.contains(articleName)) {
						resultsMap(articleName) = (resultsMap(articleName) + compare(expectedCategories, predictedCategories)) / 2.0
					}
					else {
						resultsMap += articleName -> compare(expectedCategories, predictedCategories)
					}
				}
				file.close()
				scanner.close()
			}
		}
		resultsMap
	}

	private def extractExpectedCategories(line: String) = {
		line.substring(line.indexOf('[') + 1, line.indexOf(']')).split("\t").toList
	}

	private def extractKBestArticles(line: String, k: Int) = {
		val extractor = new KNearestNeighboursExtract(k)
		val similarities = line.split("\t")
		val hasElement = { (sims: AnyRef, id: Int) ⇒ sims.asInstanceOf[Array[String]].length > id + 1 }
		val takeElement = { (sims: AnyRef, id: Int) ⇒ sims.asInstanceOf[Array[String]](id + 1) }
		val extractSimilarity = { s: String ⇒ s.substring(s.indexOf('[') + 1, s.indexOf(',')).toDouble }
		extractor.extractKNearestNeighbours(similarities, hasElement, takeElement, extractSimilarity)
	}

	private def extractPredictedCategories(bestNeighbours: Array[String])(thresholdingStrategy: (Double, Tuple2[String, Double]) ⇒ Boolean) = {
		val extractor = new NearestNeighboursCategoryExtractor
		val extractCategoriesWithSimilarityFromString = { s: String ⇒ (s.substring(s.indexOf('[') + 1, s.indexOf(',')).toDouble, s.substring(s.indexOf(',') + 1, s.indexOf(']')).split(",").toList) }
		extractor.extractCategories(bestNeighbours, extractCategoriesWithSimilarityFromString, thresholdingStrategy)
	}

	private def compare(expectedCategories: List[String], predictedCategories: List[String]): Double = {
		var n = 0
		expectedCategories.foreach { expected ⇒
			if (predictedCategories.contains(expected)) {
				n += 1
			}
		}
		((n.toDouble * 2) / expectedCategories.length * predictedCategories.length)
	}

	// DEPRECATED CODE

	@deprecated
	def validate(foldsCount: Int, learningSet: Iterable[(Int, Iterable[Word])]) {
		var matrix = divideIntoFolds(foldsCount, learningSet)
		var result = applyKNN(foldsCount, matrix)
	}

	@deprecated
	private def divideIntoFolds(foldsCount: Int, learningSet: Iterable[(Int, Iterable[Word])]): ArrayBuffer[ArrayBuffer[(Int, Iterable[Word])]] = {
		var iterator = learningSet.iterator
		var matrix = new ArrayBuffer[ArrayBuffer[(Int, Iterable[Word])]]()

		// divide learning set into test folds (naive algorithm)
		for (x ← 0 to iterator.length - 1) {
			for (y ← 0 to foldsCount) {
				if (iterator.hasNext) {
					matrix(y).append(iterator.next())
				}
			}
		}

		return matrix
	}

	@deprecated
	private def applyKNN(foldsCount: Int, matrix: ArrayBuffer[ArrayBuffer[(Int, Iterable[Word])]]): Double = {
		var learningSet = new ArrayBuffer[(Int, Iterable[Word])]()
		var categoryFinder = new CategoryFinder()
		var result = 0
		var compareKCategories = 3

		for (x ← 0 to foldsCount) {
			learningSet = extractLearningSet(x, matrix)
			for (y ← 0 to matrix.length - 1) {
				var categories = categoryFinder.findCategories(matrix(x)(y)._1)
				var predictedCategories = applyKNNForFold(matrix(x)(y)._2, learningSet)

				//				if (compare(compareKCategories, categories, predictedCategories)) result += 1
			}
		}

		return result
	}

	@deprecated
	private def extractLearningSet(fold: Int, matrix: ArrayBuffer[ArrayBuffer[(Int, Iterable[Word])]]): ArrayBuffer[(Int, Iterable[Word])] = {
		var learningSet = new ArrayBuffer[(Int, Iterable[Word])]()

		for (x ← 0 to matrix.length - 1) {
			if (x != fold) {
				for (y ← 0 to matrix(x).length - 1) {
					learningSet.append(matrix(x)(y))
				}
			}
		}

		return learningSet
	}

	// should return categories
	@deprecated
	private def applyKNNForFold(entity1: Iterable[Word], learningSet: Iterable[(Int, Iterable[Word])]): List[String] = {
		//    var knn = new CosineSimilarityIndexCount()
		//    var nearestNeighbours = knn.

		return List("Nauka", "Technika", "Religia")
	}
}

