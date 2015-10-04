package pl.gda.pg.eti.kask.kaw

import org.junit.Test
import org.junit.Assert
import org.apache.commons.io.FileUtils
import java.io.File

class WordCountTest {
	private val inputFile = "src/test/resources/input/WordCountTest"
	private val outputDirectory = "src/test/resources/output"
	private val outputFile = outputDirectory + "/part-r-00000"
	private val expectedResults = Array("Word1", "Count1", "Test1")

	@Test
	def shouldCountWordsInTestFile(): Unit = {

		FileUtils.deleteDirectory(new File(outputDirectory));
		val task = new WordCountTask

		task.runTask(Array(inputFile, outputDirectory))
		val fileLines = scala.io.Source.fromFile(outputFile).getLines

		assert(fileLines.forall { e =>
			expectedResults.contains(e.replaceAll("[\\s\\t]+", ""))
		})
	}
}