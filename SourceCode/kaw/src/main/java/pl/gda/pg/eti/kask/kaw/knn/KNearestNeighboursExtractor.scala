package pl.gda.pg.eti.kask.kaw.knn

class KNearestNeighboursExtract(private val k: Int) {

	def extractKNearestNeighbours(bestFromPreviousStep: Array[String], stream: AnyRef, hasElement: ((AnyRef, Int) ⇒ Boolean), takeElement: ((AnyRef, Int) ⇒ String), extractSimilarity: (String ⇒ Double)): Array[String] = {
		var best = bestFromPreviousStep
		var min = (0, 0.0)
		if(best == null) {
		  best = Array.fill(k) { "" }
		}
		else {
		  min = findMinimumWithIndex(best)
		}
	  
		var lastIndex = 0
		while (hasElement(stream, lastIndex)) {
			val nextLine = takeElement(stream, lastIndex)
			val similarity = extractSimilarity(nextLine)
			if (similarity > min._2) {
				best(min._1) = nextLine
				min = findMinimumWithIndex(best)
			}
			lastIndex += 1
		}
		best
	}

	private def findMinimumWithIndex(best: Array[String]): Tuple2[Int, Double] = {
		best.zipWithIndex.foldLeft[Tuple2[Int, Double]]((0, Integer.MAX_VALUE)) { (last, x) ⇒
			if (!x._1.isEmpty) {
				val sim = x._1.split("\t")(1).toDouble
				if (sim < last._2) (x._2, sim) else last
			}
			else {
			  (x._2, 0.0)
			}
		}
	}

}