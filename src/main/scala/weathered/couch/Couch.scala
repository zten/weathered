package weathered.couch

import org.ektorp.http.StdHttpClient
import org.ektorp.impl.{StdCouchDbConnector, StdCouchDbInstance}

/**
 * Couch connector
 *
 * @author Christopher Childs
 * @version 1.0, 5/18/12
 */

class Couch(val url:String = "http://localhost:5984/", val dbName:String = "default") {
  val client = new StdHttpClient.Builder().url(url).build()
  val instance = new StdCouchDbInstance(client)
  val db = new StdCouchDbConnector(dbName, instance)

  db.createDatabaseIfNotExists()

}
