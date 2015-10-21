package pl.gda.pg.eti.kask.kaw.dump

import javax.xml.parsers.SAXParser
import javax.xml.parsers.SAXParserFactory
import org.xml.sax.Attributes
import org.xml.sax.SAXException
import org.xml.sax.helpers.DefaultHandler
import scala.util.matching.Regex

class ArticleReader(private val xmlFile: String) {

  def readAndUpload() {
    try {
      val factory = SAXParserFactory.newInstance
      val parser = SAXParser.newSAXParser

      val handler = new ArticleWikiHandler
      parser.parse(xmlFile, handler)
    }
    catch {
      case e: Exception => e.printStackTrace
    }
  }

}

class ArticleWikiHandler extends DefaultHandler {

  private var articleName = ""
  private var categories = List[String]
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
      cutCategories
      // TODO: wysylanie
    }
    if(qName.equalsIgnoreCase("title")) {
      onTitleElement = false
      title = builder.toString
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

  private def cutCategories {
    val reg = """(\[\[Kategoria:.+\]\])""".r
    val all = reg.findAllIn(text)
    all.foreach { x => getCategoryAndInsert(x) }
    text = reg.replaceAllIn(text, "")
  }

  private def getCategoryAndInsert(categoryString: String) {
    val start = """(\[\[Kategoria:)""".r
    val end = """(\]\])""".r
    val category = start.replaceAllIn(end.replaceAllIn(categoryString, ""), "")
    categories = categories :+ category
  }

}
