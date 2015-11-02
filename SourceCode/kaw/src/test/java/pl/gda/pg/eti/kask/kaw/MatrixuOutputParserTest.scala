package pl.gda.pg.eti.kask.kaw

import org.junit.Test
import org.junit.Assert
import pl.gda.pg.eti.kask.kaw.extract.MatrixuOutputParser

class MatrixuOutputParserTest {
	@Test
	def shouldParse() {
	  val parser = new MatrixuOutputParser
	  var line = "1# 1-3 3-5 5-12 34-5"
	  
	  parser.parse(line)
	}
}