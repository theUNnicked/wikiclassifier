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
import org.apache.hadoop.conf.Configuration

class ArticleReader(private val xmlFile: String, private val outputFolder: String, private val local: Boolean) {

  def readAndUpload() {
    try {
      val factory = SAXParserFactory.newInstance
      val parser = factory.newSAXParser

      // TODO: local
      var file = new File(outputFolder)
      if(file.exists()) {
        if(file.isDirectory {
          FileUtils.deleteDirectory(file)
        }
        else {
          throw new Exception("File exists and is not a directory.")
        }
      }
      else {
        file.mkdir
      }

      val handler = new ArticleWikiHandler(outputFolder)
      parser.parse(xmlFile, handler)
    }
    catch {
      case e: Exception => e.printStackTrace
    }
  }

}

class ArticleWikiHandler(private val outputFolder: String) extends DefaultHandler {

  private var articleName = ""
  private var text = ""
  private var onTextElement = false
  private var onTitleElement = false
  private val builder = new StringBuilder

  @throws[SAXException]
  override def startElement(uri: String, localName: String, qName: String, attributes: Attributes)  {
    if(qName.equalsIgnoreCase("text")) {
      onTextElement = true
    }
    if(qName.equalsIgnoreCase("title")) {
      onTitleElement = true
    }
  }

  @throws[SAXException]
  override def endElement(uri: String, localName: String, qName: String)  {
    if(qName.equalsIgnoreCase("text")) {
      onTextElement = false
      text = builder.toString
      builder.setLength(0)
      var newfile = articleName.replace("/", "_").replace("\\", "_").replace(" ", "_").replace("\"", "");
      new PrintWriter(outputFolder + "/" + newfile) { write(text); close }
      //cutCategories
      // TODO: wysylanie
    }
    if(qName.equalsIgnoreCase("title")) {
      onTitleElement = false
      articleName = builder.toString
      builder.setLength(0)
    }
  }

  @throws[SAXException]
  override def characters(ch: Array[Char], start: Int, length: Int) {
    if(!onTextElement && !onTitleElement) {
      return;
    }

    val string = new String(ch, start, length)
    builder.append(string)
  }



}

class DistributedArticleReader(private val xmlFile: String, private val outputFolder: String, private val configuration: Configuration) {

  def readAndUpload() {
    try {
      val factory = SAXParserFactory.newInstance
      val parser = factory.newSAXParser

      val hdfs = FileSystem.get(configuration)
      val homeDir = hdfs.getHomeDirectory

      // TODO: local
      var file = new Path(outputFolder)
      if(hdfs.exists(file)) {
        if(hdfs.isDirectory(file)) {
          hdfs.delete(file, true)
        }
        else {
          throw new Exception("File exists and is not a directory.")
        }
      }
      else {
        file.mkdirs(file)
      }

      val handler = new ArticleWikiHandler(outputFolder)
      parser.parse(xmlFile, handler)
    }
    catch {
      case e: Exception => e.printStackTrace
    }
  }


}

class DistributedArticleWikiHandler(private val outputFolder: String, private val clusterConfiguration: Configuration, private val hdfs FileSystem) extends DefaultHandler {

  private var articleName = ""
  private var text = ""
  private var onTextElement = false
  private var onTitleElement = false
  private val builder = new StringBuilder

  @throws[SAXException]
  override def startElement(uri: String, localName: String, qName: String, attributes: Attributes)  {
    if(qName.equalsIgnoreCase("text")) {
      onTextElement = true
    }
    if(qName.equalsIgnoreCase("title")) {
      onTitleElement = true
    }
  }

  @throws[SAXException]
  override def endElement(uri: String, localName: String, qName: String)  {
    if(qName.equalsIgnoreCase("text")) {
      onTextElement = false
      text = builder.toString
      builder.setLength(0)
      var newfile = articleName.replace("/", "_").replace("\\", "_").replace(" ", "_").replace("\"", "")
      val outStream = hdfs.create(outputFolder + "/" + newfile)
      out.write(text)
      out.close

      //cutCategories
      // TODO: wysylanie
    }
    if(qName.equalsIgnoreCase("title")) {
      onTitleElement = false
      articleName = builder.toString
      builder.setLength(0)
    }
  }

  @throws[SAXException]
  override def characters(ch: Array[Char], start: Int, length: Int) {
    if(!onTextElement && !onTitleElement) {
      return;
    }

    val string = new String(ch, start, length)
    builder.append(string)
  }

}
