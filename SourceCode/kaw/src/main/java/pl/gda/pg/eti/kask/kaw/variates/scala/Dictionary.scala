package pl.gda.pg.eti.kask.kaw.variates.scala

import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.InputStream
import java.io.FileInputStream
import java.util.TreeSet
import java.io.BufferedWriter
import java.io.FileWriter
import scala.collection.JavaConversions._
import java.io.IOException
import java.io.BufferedReader
import java.io.FileReader
import java.util.HashSet
import org.slf4j.LoggerFactory

@deprecated(message = "Dobra wersja w packagu pl.gda.pg.eti.kask.kaw.variates")
object DictionaryTree {
	private val serialVersionUID = -1778581097720228638L;
	private val logger = LoggerFactory.getLogger(classOf[DictionaryTree])
}

@deprecated(message = "Dobra wersja w packagu pl.gda.pg.eti.kask.kaw.variates")
class DictionaryTree extends Serializable {
	private var dictionary: DictionaryNode = null;
	
	def serializeToFile(filePath: String) = {
		try {
			val fileOut = new FileOutputStream(filePath)
			val out = new ObjectOutputStream(fileOut)
			out.writeObject(dictionary)
			out.close
			fileOut.close
			true
		} catch {
			case e: Exception => false
		}
	}
	
	def deserializeFromFile(fileIn: InputStream): Boolean = {
		try {
			DictionaryTree.logger.debug("Rozpoczynam deserializacje")
			val in = new ObjectInputStream(fileIn)
			dictionary = in.readObject.asInstanceOf[DictionaryNode]
			in.close
			fileIn.close
			DictionaryTree.logger.debug("Deserializacja powiodla sie, otrzymano obiekt {}", dictionary)
			true
		} catch {
			case e: Exception => 
				e.printStackTrace()
				DictionaryTree.logger.error("Deserializacja nie powiodla sie: {}", e.getMessage)
				false
		}
	}

	def deserializeFromFile(filePath: String): Boolean =  {
		try {
			val fileIn = new FileInputStream(filePath)
			return deserializeFromFile(fileIn)
		} catch {
			case e: Exception => 
				DictionaryTree.logger.error("Nie udalo sie wczytac pliku: {}", e.getMessage)
				false
		}
	}

	def loadDictionaryFromFile(file: String): Boolean = {
		dictionary = new DictionaryNode(3);
		var fileDictionary: TreeSet[String] = null;
		try {
			fileDictionary = loadAndParseWordFile(file);
		} catch {
			case e: Exception => false
		}
		println("Przetworzono plik wejsciowy");
		var writer: BufferedWriter = null;
		try {
			writer = new BufferedWriter(new FileWriter("testWords.txt"));
			fileDictionary.foreach { unique =>
				writer.write(unique);
				writer.newLine();
			}
			writer.close();
		} catch {
			case e: Exception => false
		}
		println("Zapisano do pliku testWords.txt");
		fileDictionary.foreach { line =>
			val lineArray = line.split("\t");
			dictionary.insertWordWithLexem(lineArray(0), lineArray(1))
		}
		println("Utworzono slownik");
		true
	}

	def getWordLexem(word: String): String = {
		val lower = word.toLowerCase();
		val lexem = dictionary.getLexem(lower);
		if (lexem == null) lower else lexem
	}

	@throws[IOException]
	private def loadAndParseWordFile(file: String): TreeSet[String] = {
		val reader = new BufferedReader(new FileReader(file));
		val lines = new HashSet[String](10000000);
		var line = reader.readLine();
		while (line != null) {
			var newLine = "";
			val lineArray = line.split("\t");
			if (lineArray.length >= 2) {
				newLine = lineArray(0) + "\t" + lineArray(1)
				newLine = newLine.toLowerCase();
				lines.add(newLine);
			}
			line = reader.readLine()
		}
		reader.close();
		val orderedSet = new TreeSet[String]();
		orderedSet.addAll(lines);
		orderedSet
	}
}

@deprecated(message = "Dobra wersja w packagu pl.gda.pg.eti.kask.kaw.variates")
object DictionaryNode {
	private val serialVersionUID = -1335933171986936438L;
	private val logger = LoggerFactory.getLogger(classOf[DictionaryNode])
}

@deprecated(message = "Dobra wersja w packagu pl.gda.pg.eti.kio.kaw.variates")
class DictionaryNode(private val maxDepth: Int, private val depth: Int) extends Serializable{
	private var key: String = null
	private var value: String = null
	private val leaves = List[DictionaryNode]()
	
	def this(maxDepth: Int) {
		this(maxDepth, 0)
	}
	
	def insertWordWithLexem(word: String, lexem: String) {
		if(depth == word.length()) {
			value = lexem;
			key = word;
			return;
		}
		if(depth == maxDepth) {
			value = lexem;
			key = word;
		}
		else {
			leaves.foreach { leaf =>
				if(leaf.getKey.equals(word.substring(0,leaf.getDepth()))) {
					leaf.insertWordWithLexem(word, lexem);
					return;
				}
			}
			val leaf = new DictionaryNode(this.maxDepth, depth+1);
			leaf.setKey(word.substring(0,leaf.getDepth()));
			leaf.insertWordWithLexem(word, lexem);
			leaves.add(leaf);
		}
	}
	
	def getLexem(word: String): String = {
		if(leaves.isEmpty() || word.length() == depth) {
			if(word.equals(key)) {
				return this.value;
			}
			return null;
		}
		else {
			if(word.substring(0, depth).equals(key) || depth == 0) {
				leaves.foreach { leaf =>
					val lexem = leaf.getLexem(word);
					if(lexem != null) {
						return lexem;
					}
				}
				return null;
			}
			return null;
		}
	}
	
	def getKey = { key }
	
	def setKey(key: String) {
		this.key = key;
	}
	
	def getValue = { value }
	
	def setValue(value: String) {
		this.value = value;
	}
	
	def getDepth() = { depth }
}
