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
package pro.foundev.cassandra

import com.datastax.driver.core.{Row, SimpleStatement}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pro.foundev.Configuration._
import pro.foundev.dto.IpLog

import scala.collection.JavaConverters._

class CassandraRepository  extends Serializable{
  private def mapLog(result:Row): IpLog = {
    val id = result.getLong("id")
    val originState = result.getString("origin_state")
    val ipAddress = result.getInet("ip_address")
    val urls = result.getList[String]("urls", classOf[String])
    new IpLog(id, originState, ipAddress, urls.asScala.toList)
  }

  private def mapLogFromResult(result: CassandraRow): IpLog ={
    val id = result.getLong("id")
    val originState = result.getString("origin_state")
    val ipAddress = result.getInet("ip_address")
    val urls = result.getList[String]("urls")
    new IpLog(id, originState, ipAddress, urls.toList)
  }

  def getLogsForIds(ids: Set[Long], connector: CassandraConnector): List[IpLog] = {
    //without primary key pushdown in 1.1 use of cassandra connector is necessary
    var logs: List[IpLog] = null
    connector.withSessionDo { session => {
      val result = ids.map(i => {
        val statement = new SimpleStatement("SELECT * FROM %s.%s WHERE id = %d".format(getKeyspace, getTable, i))
        session.executeAsync(statement)
      }).map(future => future.getUninterruptibly().one())
        .filter(r => r != null)
        .map(result => mapLog(result))
      logs = result.toList
    }}
    logs
  }

  def getLogForId(logId: Long, connector: CassandraConnector): IpLog = {
      //without primary key pushdown in 1.1 this is necessary
    var log: IpLog = null
    connector.withSessionDo { session =>{
      val statement = new SimpleStatement("SELECT * FROM %s.%s WHERE id = %d".format(getKeyspace, getTable,logId))
      val results = session.execute(statement)
      val result = results.one()
      log = mapLog(result)
      }
    }
    log
  }

  def getLogsFromRandom2i(state: String, sparkContext: SparkContext): RDD[IpLog] = {
    sparkContext.cassandraTable(getKeyspace, getTable)
    .where("origin_state = ?", state)
      .map(result => mapLogFromResult(result))
  }

  def getLogsFromRandom2iUsingConnector(state: String, connector: CassandraConnector):List[IpLog] = {
    //without primary key pushdown in 1.1 use of cassandra connector is necessary
    var logs: List[IpLog] = null
    connector.withSessionDo { session => {
      val statement = new SimpleStatement("SELECT * FROM %s.%s WHERE origin_state = '%s'".format(getKeyspace, getTable, state))
      val results = session.execute(statement).all().asScala.toList
      logs = results.map(row => mapLog(row))
    }}
    logs
  }

}
