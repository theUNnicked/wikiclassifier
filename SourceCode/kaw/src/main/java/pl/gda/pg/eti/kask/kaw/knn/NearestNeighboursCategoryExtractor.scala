package pl.gda.pg.eti.kask.kaw.knn

import scala.collection.mutable.MutableList

class NearestNeighboursCategoryExtractor {

	def extractCategories(kNearestNeighbrours: Array[String], extractCategoriesWithSimilarityFromString: (String ⇒ (Double, List[String])), passedByThresholdingStrategy: (Double, Tuple2[String, Double]) ⇒ Boolean): List[String] = {
		val map = scala.collection.mutable.Map[String, Double]()
		kNearestNeighbrours.foreach { x ⇒
			if (x != null && !x.isEmpty) {
			  // TODO: tu jest blad
				val categoriesWithSimilarity = extractCategoriesWithSimilarityFromString(x)
				if(categoriesWithSimilarity != null) {
  				val similarity = categoriesWithSimilarity._1
  				categoriesWithSimilarity._2.foreach { cat ⇒
  					if (map.contains(cat)) {
  						map(cat) += similarity
  					}
  					else {
  						map += cat -> similarity
  					}
  				}
				}
			}
		}
		val seq = map.toSeq
		val max = seq.maxBy(_._2)._2
		seq.foldLeft(MutableList[String]()) { (list, el) ⇒
			if (passedByThresholdingStrategy(max, el)) {
				list += el._1
			}
			list
		}.toList
	}

}