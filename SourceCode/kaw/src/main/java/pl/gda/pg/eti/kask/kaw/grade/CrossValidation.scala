package pl.gda.pg.eti.kask.kaw.grade

import scala.collection.mutable.ArrayBuffer
import pl.gda.pg.eti.kask.kaw.extract.Word
import pl.gda.pg.eti.kask.kaw.extract.CategoryFinder
import pl.gda.pg.eti.kask.kaw.knn.KnnClassifier


class CrossValidation {
  
  def validate(foldsCount: Int, learningSet: Iterable[(Int, Iterable[Word])]) {
    var matrix = divideIntoFolds(foldsCount, learningSet)
    var result = applyKNN(foldsCount, matrix)
  }
  
  private def divideIntoFolds(foldsCount: Int, learningSet: Iterable[(Int, Iterable[Word])]) : ArrayBuffer[ArrayBuffer[(Int, Iterable[Word])]] ={
    var iterator = learningSet.iterator
    var matrix = new ArrayBuffer[ArrayBuffer[(Int, Iterable[Word])]]()
    
    // divide learning set into test folds (naive algorithm)
    for (x <- 0 to iterator.length - 1) {
      for (y <- 0 to foldsCount) {
        if (iterator.hasNext) {
          matrix(y).append(iterator.next()) 
        }  
      }
    }
    
    return matrix
  }
  
  private def applyKNN(foldsCount: Int, matrix: ArrayBuffer[ArrayBuffer[(Int, Iterable[Word])]]) : Double ={
    var learningSet = new ArrayBuffer[(Int, Iterable[Word])]()
    var categoryFinder = new CategoryFinder()
    var result = 0.0
    
    for (x <- 0 to foldsCount) {
      learningSet = extractLearningSet(x, matrix)
      for (y <- 0 to matrix.length - 1) {
        var categories = categoryFinder.findCategories(matrix(x)(y)._1)
        var predictedCategories = applyKNNForFold(matrix(x)(y)._2, learningSet)
        
//        compare(categories, predictedCategories)
       }
    }
    
    return result
  }
  
  private def extractLearningSet(fold: Int, matrix: ArrayBuffer[ArrayBuffer[(Int, Iterable[Word])]]) : ArrayBuffer[(Int, Iterable[Word])] ={
    var learningSet = new ArrayBuffer[(Int, Iterable[Word])]()
    
    for (x <- 0 to matrix.length - 1) {
      if (x != fold) {
         for (y <- 0 to matrix(x).length - 1) {
           learningSet.append(matrix(x)(y))
         }
      }
    }
    
    return learningSet
  }
  
  // should return categories
  private def applyKNNForFold(entity1: Iterable[Word], learningSet: Iterable[(Int, Iterable[Word])]) : List[String] ={
    var knn = new KnnClassifier()
    var nearestNeighbours = knn.getKNearestNeighbours(entity1, 2, learningSet)
    
    return List("Nauka", "Technika", "Religia")
  }
  
  private def compare(expectedCategories: List[String], predictedCategories: List[String]) : Double ={
    var compareResult = 0.0
    
    // TODO: compare known categories with the ones that are results from knn
    
    return compareResult
  }
}

