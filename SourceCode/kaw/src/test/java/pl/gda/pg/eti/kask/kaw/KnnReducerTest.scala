package pl.gda.pg.eti.kask.kaw

import scala.collection.JavaConversions._
import org.junit.Test
import org.junit.Assert

class KnnReducerTest {
	
	val objectUnderTest = new KnnReducer
  
	val sofa = new PairWritable("sofa", "1")
	val chair = new PairWritable("krzeslo", "2")
	
	val category = new PairWritable("::Cat", "Dom")
	
	@Test
	def shouldExtractCategoriesAndWords {
		var input = List[PairWritable](sofa, category, chair);
		val unpacked = objectUnderTest.unpack(input)
		
		val words = unpacked._1
		val cats = unpacked._2
		
		Assert.assertEquals(words(0).getWord, "sofa")
		Assert.assertEquals(words(0).getCount, 1)
		Assert.assertEquals(words(1).getWord, "krzeslo")
		Assert.assertEquals(words(1).getCount, 2)
		
		Assert.assertEquals(cats(0), "Dom")
	}
	
}