package pl.gda.pg.eti.kask.kaw.knn
import pl.gda.pg.eti.kask.kaw.Word
import scala.collection.mutable.ArrayBuffer

class CosineDistance {
  private var vector1 = new ArrayBuffer[Int]()
  private var vector2 = new ArrayBuffer[Int]()
  
  def getDistance(entity1 : Iterable[Word], entity2 : Iterable[Word]) : Double ={
    var dot = 0
    var quantity1 = 0.0
    var quantity2 = 0.0
    
    setVectors(entity1, entity2)
    val matrixMultiplied = vector1.toList.zip(vector2)
    
    for(elem <- matrixMultiplied) dot += elem._1*elem._2
    for(elem <- vector1) quantity1 += scala.math.pow(elem, 2)
    for(elem <- vector2) quantity2 += scala.math.pow(elem, 2)
    
    return dot/(scala.math.sqrt(quantity1) * scala.math.sqrt(quantity2))
  }
  
  private def setVectors(iterable1: Iterable[Word], iterable2: Iterable[Word]) : Unit ={
    var all_words = new ArrayBuffer[String]()
    
    var iterator1 = iterable1.iterator
    while(iterator1.hasNext) {
      var wordId = iterator1.next.getWord
      if(!all_words.contains(wordId)) all_words += wordId
    }
    
    var iterator2 = iterable2.iterator
    while(iterator2.hasNext) {
      var wordId = iterator2.next.getWord
      if(!all_words.contains(wordId)) all_words += wordId
    }
    
    for (x <- 0 to all_words.length-1) {
      var iterable1Elem = iterable1.find(_.getWord == all_words(x))
      if(iterable1Elem.isDefined) vector1 += iterable1Elem.get.getCount else vector1 += 0    
      
      var iterable2Elem = iterable2.find(_.getWord == all_words(x))
      if(iterable2Elem.isDefined) vector2 += iterable2Elem.get.getCount else vector2 += 0
    }
  }
}