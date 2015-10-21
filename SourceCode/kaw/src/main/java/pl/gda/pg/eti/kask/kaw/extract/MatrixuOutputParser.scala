package pl.gda.pg.eti.kask.kaw.extract


import java.io.DataOutput
import java.io.DataInput
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.mapreduce.Mapper
import java.io.IOException

class MatrixuOutputParser {
  
	def parse(line: String, context: Mapper[Object, Text, IntWritable, Word]#Context) {
		// ======================================== //
		// TODO wyciagnij z linijki artykul i slowo //
		// razem z wystepowaniem					//
//		val article = 0
//		var word = new Word(0, 0)
		// ======================================== //
		
		val tmpArticleWords = line.split("# ")
		
		val article = tmpArticleWords(0).toInt
		val key = new IntWritable
		key.set(article)
		
		val tmpWords = tmpArticleWords(1).split(" ").map {
		  x =>
		  val value = x.split("-")
		  val word = new Word(value(0).toInt, value(1).toInt)
		  context.write(key, word)
		}
	}
	
	def parse(line: String) {
		val tmpArticleWords = line.split("# ")
		
		val article = tmpArticleWords(0).toInt
		val tmpWords = tmpArticleWords(1).split(" ").map {
		  x =>
		  val value = x.split("-")
		  val word = new Word(value(0).toInt, value(1).toInt)
		  println(word.getId)
		  println(word.getCount)
		}
	}
}

class Word(private var id: Int, private var count: Int) extends Writable {
	def getId = { id }
	def getCount = { count }
	
	@throws[IOException]
	override def write(out: DataOutput) {
		val idWritable = new IntWritable();
		val countWritable = new IntWritable();
		
		idWritable.set(id)
		idWritable.write(out)
		
		countWritable.set(count)
		countWritable.write(out)
	}
	
	@throws[IOException]
	override def readFields(in: DataInput) {
		val idWritable = new IntWritable();
		val countWritable = new IntWritable();
		
		idWritable.readFields(in)
		id = idWritable.get
		
		countWritable.readFields(in)
		count = countWritable.get
	}
}