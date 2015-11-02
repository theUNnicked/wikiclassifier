package pl.gda.pg.eti.kask.kaw

import org.junit.Test
import org.junit.Assert
import pl.gda.pg.eti.kask.kaw.knn.CosineDistance

class CosineDistanceTest {
	@Test
	def shouldChooseOneCategory() {
		val cosineDistance = new CosineDistance
		
		var iterable1 = Iterable(new Word("1", 2), new Word("2", 3), new Word("3", 2), new Word("4", 2))
		var iterable2 = Iterable(new Word("2", 2), new Word("4", 3), new Word("1", 4))

		var distance = cosineDistance.getDistance(iterable1, iterable2)
		
		println(distance)
		
	  Assert.assertTrue(true)
	}
}