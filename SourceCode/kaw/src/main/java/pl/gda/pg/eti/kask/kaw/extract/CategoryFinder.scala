package pl.gda.pg.eti.kask.kaw.extract

import java.sql.Connection
import java.sql.DriverManager

class CategoryFinder {
  
	private val CONNECTION_STRING = "jdbc:h2:~/test"
	private val USER = "sa"
	private val PASSWORD = ""
	
	private var connection: Connection = _
	
	def findCategories(articleId: Int): List[String] = {
		// ======================================== //
		// TODO do uzupelnienia luki				//
		try {
			connectToDb()
			val statement = connection.createStatement()
			// TODO zapytanie do bazy danych
			val query = "SELECT ... FROM .. JOIN .. "
			val resultSet = statement.executeQuery(query)
			while(resultSet.next()) {
				val id = resultSet.getInt("ID")
				// TODO pobranie reszty pol z bazy danych analogicznie
			}
			connection.close
		}
		catch {
			case e: Exception => //TODO obsluz wyjatek
		}
		// TODO zwroc wynik wyszukiwania
		null
		// ======================================== //
	}
	
	private def connectToDb() {
		Class.forName("org.h2.Driver")
       	connection = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD)
	}
}