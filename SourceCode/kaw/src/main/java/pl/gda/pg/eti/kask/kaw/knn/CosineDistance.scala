package pl.gda.pg.eti.kask.kaw.knn
import pl.gda.pg.eti.kask.kaw.cluster.Word
import scala.collection.mutable.ArrayBuffer
import scala.math.pow

class CosineDistance {
  private var vector1 = new ArrayBuffer[Int]()
  private var vector2 = new ArrayBuffer[Int]()
  
  def getDistance(entity1 : Iterable[Word], entity2 : Iterable[Word]) : Double ={
    var dot = 0
    var quantity1 = 0.0
    var quantity2 = 0.0
    
    val vectorMap = createVectors(entity1, entity2)
    vectorMap.foreach { x => 
      val k = x._1
      val v = x._2
      
      dot += v._1 * v._2
      quantity1 += pow(v._1, 2)
      quantity2 += pow(v._2, 2)
    }
    
    return dot/(scala.math.sqrt(quantity1) * scala.math.sqrt(quantity2))
  }
  
  private def createVectors(iterable1: Iterable[Word], iterable2: Iterable[Word])= {
    val vectorMap = scala.collection.mutable.Map[String, Tuple2[Int, Int]]()
    iterable1.foreach { x => 
      vectorMap += x.getWord -> (x.getCount, 0)
    }
    
    iterable2.foreach { x =>
      if(vectorMap.contains(x.getWord)) {
        vectorMap(x.getWord) = (vectorMap(x.getWord)._1, x.getCount)
      }
      else {
        vectorMap += x.getWord -> (0, x.getCount)
      }
    }
    
    vectorMap
  }
}