/*
 *  Copyright 2015 Foundational Development
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package pro.foundev.examples.spark_streaming.cassandra

import com.datastax.bdp.spark.DseSparkConfHelper
import com.datastax.spark.connector.cql.CassandraConnector
import scala.collection.JavaConversions._
import com.datastax.spark.connector.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import DseSparkConfHelper._

trait CassandraCapable {

  val keySpaceName =  "tester"
  val fullTableName =  "streaming_demo"
  var withAuth = false
  val password = "cassandra"
  val username = "cassandra"

  def connect(master: String): CassandraContext = {

    withAuth = true
    var conf = (new SparkConf()
      .set("spark.cassandra.connection.host", master)
      .setMaster("spark://"+ master+":7077")
      .setAppName("Windowed_Rapid_Transaction_Check")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "cfs:///spark_logs/")
      )
    if (withAuth){
      conf = conf.set("spark.cassandra.auth.username", username)
      .set("spark.cassandra.auth.password", password)
    }
    conf = conf.forDse

    val connector = CassandraConnector(conf)
    connector.withSessionDo(session => {
      session.execute(s"create keyspace if not exists ${keySpaceName} with replication = { 'class':'SimpleStrategy', " +
        "'replication_factor':1}")
      session.execute(s"drop table if exists ${keySpaceName}.${fullTableName}")
      session.execute(s"create table if not exists ${keySpaceName}.${fullTableName} " +
        "(userId int, userName text, followers Set<text>, PRIMARY KEY(userId))")
      val preparedStatement = session.prepare(s"INSERT INTO ${keySpaceName}.${fullTableName} (userId, userName, " +
        s"followers) values (?,?,?)")
      session.execute(preparedStatement.bind(0: Integer, "jsmith", setAsJavaSet(Set("jsmith", "mark", "mike"))))
      session.execute(preparedStatement.bind(1: Integer, "mark", setAsJavaSet(Set("mark", "mike"))))
      session.execute(preparedStatement.bind(2: Integer, "mike", setAsJavaSet(Set("jsmith", "mike"))))
    })

    val ssc = new StreamingContext(conf, Milliseconds(5000))
    new CassandraContext(connector, ssc.cassandraTable(keySpaceName, fullTableName), ssc)
  }
}
