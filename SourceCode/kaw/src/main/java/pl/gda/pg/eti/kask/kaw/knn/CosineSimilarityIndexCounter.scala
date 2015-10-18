package pl.gda.pg.eti.kask.kaw.knn

import pl.gda.pg.eti.kask.kaw.extract.Word
import pl.gda.pg.eti.kask.kaw.extract.CategoryFinder
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

class CosineSimilarityIndexCounter {
	
	private val categoryFinder = new CategoryFinder

	def countSimilarity(articlesIds: Iterable[Int], wordsWithCount: Iterable[Word], context: Reducer[IntWritable, Word, Text, IntWritable]#Context) {
	  var iterator = articlesIds.iterator
	  var categoriesMap = scala.collection.mutable.Map[String, Int]()
	  while (iterator.hasNext) {
	    // TODO zmiana funkcji z tymczasowej na właściwą
	    val categories = categoryFinder.findCategories(iterator.next)
      for (x <- 0 to categories.length-1) {
        if(!categoriesMap.contains(categories(x))) {
          categoriesMap += (categories(x) -> 1)
        } else {
          categoriesMap(categories(x)) += 1
        }
      }
	  }
	  val predictedClasses = categoriesMap.toSeq.sortBy(-_._2).take(3)
    println( predictedClasses )
		
		// TODO Zapisanie do contextu
		//context.write(new Text(name), new IntWritable(count))
	}
}