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

package pro.foundev.scala

/*
 * Copyright 2014 Foundational Development
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}

trait CassandraCapable {

  val keySpaceName: String = "spark_bulk_ops"
  val primaryTableName: String = "kv"

  def getPrimaryTableName(): String = {
    primaryTableName
  }

  def getKeySpaceName(): String = {
    keySpaceName
  }

  def getFullTableName(): String = {
    s"${getKeySpaceName()}.${getPrimaryTableName()}"
  }

  def connectToCassandra(): CassandraContext = {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext("spark://127.0.0.1:7077", "test", conf)
    val connector = CassandraConnector(conf)
    connector.withSessionDo(session => {
      session.execute(s"create keyspace if not exists ${getKeySpaceName()} with replication = { 'class':'SimpleStrategy', " +
        "'replication_factor':1}")
      session.execute(s"create table if not exists ${getFullTableName()} " +
        "(marketId text, id int, version int, value text, PRIMARY KEY((id), version))")
    })
    val rdd = sc.cassandraTable(getKeySpaceName(), getPrimaryTableName())
    new CassandraContext(connector, rdd, sc)
  }
}
