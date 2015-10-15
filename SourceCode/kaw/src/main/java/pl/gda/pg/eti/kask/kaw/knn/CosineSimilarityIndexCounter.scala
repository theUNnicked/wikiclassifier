package pl.gda.pg.eti.kask.kaw.knn

import pl.gda.pg.eti.kask.kaw.extract.Word
import pl.gda.pg.eti.kask.kaw.extract.CategoryFinder
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

class CosineSimilarityIndexCounter {
	
	private val categoryFinder = new CategoryFinder
	
	def countSimilarity(articleId: Int, wordsWithCount: Iterable[Word], context: Reducer[IntWritable, Word, Text, IntWritable]#Context) {
		// TODO wyznaczanie najblizszych artykulow
		val categories = categoryFinder.findCategories(articleId)
		// TODO wyznaczenie najlepszych kategorii i zapisanie do kontekstu
		val name = "<nazwa_kategorii>"
		val count = 15 // liczba powtorzen
		context.write(new Text(name), new IntWritable(count))
	}
	
}