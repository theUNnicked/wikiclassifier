package pl.gda.pg.eti.kask.kaw.knn
import pl.gda.pg.eti.kask.kaw.extract.Word
import scala.collection.mutable.ArrayBuffer

@deprecated(message = "Nie uzywany na rzecz NoMatrixuSimilarityTask")
class KnnClassifier {
  def getKNearestNeighbours(entity1: Iterable[Word], k : Int, learningSet: Iterable[(Int, Iterable[Word])]) : ArrayBuffer[Int] ={
    var cosineDistance = new CosineDistance()
    var distances = new ArrayBuffer[(Int, Double)]()
    var iterator = learningSet.iterator
    var nearestNeighbours = ArrayBuffer[Int]()
    
    while(iterator.hasNext) {
      var learningCase = iterator.next
//      var dist: Double = cosineDistance.getDistance(entity1, learningCase._2)
//      distances += Tuple2(learningCase._1, dist)
    }
    
    var sortedDistances = distances.sortBy(_._2)
    for (x <- 0 to k-1) {
      nearestNeighbours += sortedDistances(x)._1
    }
    
    return nearestNeighbours
  }
}