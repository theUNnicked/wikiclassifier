package pl.gda.pg.eti.kask.kaw.dump

import javax.xml.parsers.SAXParser
import javax.xml.parsers.SAXParserFactory
import org.xml.sax.Attributes
import org.xml.sax.SAXException
import org.xml.sax.helpers.DefaultHandler
import scala.util.matching.Regex
import org.apache.commons.io.FileUtils
import java.io.File
import java.io.PrintWriter
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import java.io.BufferedWriter
import java.io.OutputStreamWriter

class ArticleReader(private val xmlFile: String, private val outputFolder: String) {

	def readAndUpload() {
		try {
			val factory = SAXParserFactory.newInstance
			val parser = factory.newSAXParser

			ArticleReader.logger.debug("Tworzenie lokalnego folderu {}", outputFolder)
			var file = new File(outputFolder)
			if (file.exists()) {
				if (file.isDirectory) {
					ArticleReader.logger.debug("Folder {} istnieje, usuwam z zawartoscia", outputFolder)
					FileUtils.deleteDirectory(file)
				}
				else {
					throw new Exception("File exists and is not a directory.")
				}
			}
			else {
				file.mkdir
				ArticleReader.logger.debug("Udalo sie stworzyc folder {}", outputFolder)
			}

			val handler = new ArticleWikiHandler(outputFolder)
			ArticleReader.logger.debug("Rozpoczynam wczytywanie pliku {}", xmlFile)
			parser.parse(xmlFile, handler)
			ArticleReader.logger.debug("Zakonczylem wczytywanie pliku {}", xmlFile)
		}
		catch {
			case e: Exception ⇒ e.printStackTrace
		}
	}

}

object ArticleReader {
	private val logger = LoggerFactory.getLogger(classOf[ArticleReader])
}
object ArticleWikiHandler {
	private val logger = LoggerFactory.getLogger(classOf[ArticleWikiHandler])
}
object DistributedArticleReader {
	private val logger = LoggerFactory.getLogger(classOf[ArticleReader])
}
object DistributedArticleWikiHandler {
	private val logger = LoggerFactory.getLogger(classOf[ArticleWikiHandler])
}

class ArticleWikiHandler(private val outputFolder: String) extends DefaultHandler {

	private var articleName = ""
	private var text = ""
	private var onTextElement = false
	private var onTitleElement = false
	private val builder = new StringBuilder
	private var counter = 0

	@throws[SAXException]
	override def startElement(uri: String, localName: String, qName: String, attributes: Attributes) {
		if (counter % 250 == 0) {
			ArticleWikiHandler.logger.debug("Liczba przetworzonych artykulow {}", counter.toString)
		}
		if (qName.equalsIgnoreCase("text")) {
			onTextElement = true
		}
		if (qName.equalsIgnoreCase("title")) {
			onTitleElement = true
		}
	}

	@throws[SAXException]
	override def endElement(uri: String, localName: String, qName: String) {
		counter = counter + 1
		if (qName.equalsIgnoreCase("text")) {
			onTextElement = false
			text = builder.toString
			builder.setLength(0)
			var newfile = articleName.replace("/", "_").replace("\\", "_").replace(" ", "_").replace("\"", "").replace(":", "_")
			new PrintWriter(outputFolder + "/" + newfile) { write(text); close }
		}
		if (qName.equalsIgnoreCase("title")) {
			onTitleElement = false
			articleName = builder.toString
			builder.setLength(0)
		}
	}

	@throws[SAXException]
	override def characters(ch: Array[Char], start: Int, length: Int) {
		if (!onTextElement && !onTitleElement) {
			return ;
		}

		val string = new String(ch, start, length)
		builder.append(string)
	}

}

class DistributedArticleReader(private val xmlFile: String, private val outputFolder: String, private val hdfs: FileSystem) {

	def readAndUpload() {
		try {
			val factory = SAXParserFactory.newInstance
			val parser = factory.newSAXParser

			DistributedArticleReader.logger.debug("Tworzenie lokalnego folderu {}", outputFolder)
			var file = new Path(outputFolder)
			if (hdfs.exists(file)) {
				if (hdfs.isDirectory(file)) {
					DistributedArticleReader.logger.debug("Folder {} istnieje, usuwam z zawartoscia", outputFolder)
					hdfs.delete(file, true)
				}
				else {
					throw new Exception("File exists and is not a directory.")
				}
			}
			else {
				hdfs.mkdirs(file)
				DistributedArticleReader.logger.debug("Udalo sie stworzyc folder {}", outputFolder)
			}

			val handler = new DistributedArticleWikiHandler(outputFolder, hdfs)
			DistributedArticleReader.logger.debug("Rozpoczynam wczytywanie pliku {}", xmlFile)
			parser.parse(xmlFile, handler)
			DistributedArticleReader.logger.debug("Zakonczylem wczytywanie pliku {}", xmlFile)
		}
		catch {
			case e: Exception ⇒ e.printStackTrace
		}
	}
}

class DistributedArticleWikiHandler(private val outputFolder: String, private val hdfs: FileSystem) extends DefaultHandler {

	private var articleName = ""
	private var text = ""
	private var onTextElement = false
	private var onTitleElement = false
	private val builder = new StringBuilder
	private var counter = 0

	@throws[SAXException]
	override def startElement(uri: String, localName: String, qName: String, attributes: Attributes) {
		if (qName.equalsIgnoreCase("text")) {
			onTextElement = true
			if (counter % 250 == 0) {
				DistributedArticleWikiHandler.logger.debug("Liczba przetworzonych artykulow {}", counter.toString)
			}
		}
		if (qName.equalsIgnoreCase("title")) {
			onTitleElement = true
		}
	}

	@throws[SAXException]
	override def endElement(uri: String, localName: String, qName: String) {
		if (qName.equalsIgnoreCase("text")) {
			counter = counter + 1
			onTextElement = false
			text = builder.toString
			builder.setLength(0)
			var newfile = articleName.replace("/", "_").replace("\\", "_").replace(" ", "_").replace("\"", "").replace(":", "_")
			val outStream = hdfs.create(new Path(outputFolder + "/" + newfile))
			val bw = new BufferedWriter(new OutputStreamWriter(outStream))
			bw.write(text)
			bw.close
			outStream.close
		}
		if (qName.equalsIgnoreCase("title")) {
			onTitleElement = false
			articleName = builder.toString
			builder.setLength(0)
		}
	}

	@throws[SAXException]
	override def characters(ch: Array[Char], start: Int, length: Int) {
		if (!onTextElement && !onTitleElement) {
			return ;
		}

		val string = new String(ch, start, length)
		builder.append(string)
	}
}
