/*
 * Copyright 2015 Foundational Development
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

 package pro.foundev.commons.test_support

import scala.collection.mutable
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import org.scalatest._

trait CassandraSupport extends BeforeAndAfterEach {
 this: Suite =>

  var cluster: Cluster = _
  var session: Session = _
  val keyspaces: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]

  override def beforeEach(): Unit = {
    cluster = Cluster.builder()
           .addContactPoint("127.0.0.1")
                    .build()
    session = cluster.newSession
    super.beforeEach
  }

  override def afterEach(): Unit = {
    keyspaces.foreach(keyspace => cql("DROP KEYSPACE IF EXISTS " + keyspace ))
    if(cluster != null){
      cluster.close
    }
    super.afterEach
  }

  def makeTable(table:String, columns: Seq[Tuple2[String, String]], pk: String): Unit = {
    val colString: String = columns.map(x=>x._1 + " " + x._2).mkString(",")
    cql("CREATE TABLE " + table + " ( " + colString + ", PRIMARY KEY(" + pk + "))")
  }
  def makeKeyspace(keyspace:String):Unit = {
    //horrible race condition waiting to happen
    cql("DROP KEYSPACE IF EXISTS " + keyspace   )
    cql("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH REPLICATION = { 'class':'SimpleStrategy', 'replication_factor':1 } ")
    keyspaces += keyspace
  }
  def cql(cql:String):Unit = {
    session.execute(cql)
  }

 }
