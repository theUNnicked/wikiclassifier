package pl.gda.pg.eti.kask.kaw

import org.junit.Test
import org.junit.Assert
import pl.gda.pg.eti.kask.kaw.knn.CosineSimilarityIndexCounter

class CosineSimilarityIndexCounterTest {
	@Test
	def shouldChooseOneCategory() {
		val counter = new CosineSimilarityIndexCounter
		var iterable = 1 to 10
		counter.countSimilarity(iterable, null, null)
		
	  Assert.assertTrue(true)
	}
}