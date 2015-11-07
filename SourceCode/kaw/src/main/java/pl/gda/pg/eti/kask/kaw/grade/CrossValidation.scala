package pl.gda.pg.eti.kask.kaw.grade

import scala.collection.mutable.ArrayBuffer
import pl.gda.pg.eti.kask.kaw.extract.Word
import pl.gda.pg.eti.kask.kaw.extract.CategoryFinder
import pl.gda.pg.eti.kask.kaw.knn.KnnClassifier
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
//import.pl.gda.pg.eti.kask.kaw.knn.CosineSimilarityIndexCounter

class CrossValidation {

	def validate(outputDir: String, hdfs: FileSystem) {
		val resultsMap = scala.collection.mutable.Map[String, Double]()
		val files = hdfs.listStatus(new Path(outputDir));
		var i = 0
		
		// struktura pliku: artykul_test<>artykul_tren	dopasowanie	::TrainCats	kategorie_tren(z wiekszego zbioru [90/100])	::TestCats	kategorie_test(z mniejszego zbioru [10/100])
		// TODO: wczytywanie pliku
		// TODO: wybranie najlepszych kategorii (tak jak w CosineSimilarityIndexCounter)
		// TODO: zastosowanie metody compare i usrednienie wszystkich wartosci itd
		// TODO: szkielet do uzupelnienia:

//		var best = Array.fill(k) { "" }
//		var min = (0, 0.0)
//
//		while (i < files.length) {
//			if(!files(i).getPath.toString.contains(".crc")) {
//				val file = hdfs.open(files(i).getPath)
//				val bin = new BufferedReader(new InputStreamReader(file))
//				Stream.continually(bin.readLine).takeWhile(_ != null).foreach { line =>
//					if(line != null && !line.isEmpty()) {
//						if (line.split("\t")(1).toDouble > min._2) {
//							best(min._1) = line
//							min = findMinimumWithIndex(best)
//						}
//					}
//				}
//				bin.close
//				file.close
//			}
//			i = i + 1
//		}
	}

	def validate(foldsCount: Int, learningSet: Iterable[(Int, Iterable[Word])]) {
		var matrix = divideIntoFolds(foldsCount, learningSet)
		var result = applyKNN(foldsCount, matrix)
	}

	private def divideIntoFolds(foldsCount: Int, learningSet: Iterable[(Int, Iterable[Word])]): ArrayBuffer[ArrayBuffer[(Int, Iterable[Word])]] = {
		var iterator = learningSet.iterator
		var matrix = new ArrayBuffer[ArrayBuffer[(Int, Iterable[Word])]]()

		// divide learning set into test folds (naive algorithm)
		for (x <- 0 to iterator.length - 1) {
			for (y <- 0 to foldsCount) {
				if (iterator.hasNext) {
					matrix(y).append(iterator.next())
				}
			}
		}

		return matrix
	}

	private def applyKNN(foldsCount: Int, matrix: ArrayBuffer[ArrayBuffer[(Int, Iterable[Word])]]): Double = {
		var learningSet = new ArrayBuffer[(Int, Iterable[Word])]()
		var categoryFinder = new CategoryFinder()
		var result = 0
		var compareKCategories = 3

		for (x <- 0 to foldsCount) {
			learningSet = extractLearningSet(x, matrix)
			for (y <- 0 to matrix.length - 1) {
				var categories = categoryFinder.findCategories(matrix(x)(y)._1)
				var predictedCategories = applyKNNForFold(matrix(x)(y)._2, learningSet)

//				if (compare(compareKCategories, categories, predictedCategories)) result += 1
			}
		}

		return result
	}

	private def extractLearningSet(fold: Int, matrix: ArrayBuffer[ArrayBuffer[(Int, Iterable[Word])]]): ArrayBuffer[(Int, Iterable[Word])] = {
		var learningSet = new ArrayBuffer[(Int, Iterable[Word])]()

		for (x <- 0 to matrix.length - 1) {
			if (x != fold) {
				for (y <- 0 to matrix(x).length - 1) {
					learningSet.append(matrix(x)(y))
				}
			}
		}

		return learningSet
	}

	// should return categories
	private def applyKNNForFold(entity1: Iterable[Word], learningSet: Iterable[(Int, Iterable[Word])]): List[String] = {
		//    var knn = new CosineSimilarityIndexCount()
		//    var nearestNeighbours = knn.

		return List("Nauka", "Technika", "Religia")
	}

	private def compare(k: Int, expectedCategories: List[String], predictedCategories: List[String]): Double = {
		var n = 0
		expectedCategories.foreach { expected =>
			if (predictedCategories.contains(expected)) {
				n += 1
			}
		}
		((n.toDouble * 2) / expectedCategories.length * predictedCategories.length)
	}
}

