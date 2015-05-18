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

package pro.foundev.cassandra

import java.net.InetAddress

import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSpec, Matchers}
import pro.foundev.Configuration
import pro.foundev.commons.test_support.SparkSpec
import pro.foundev.dto.IpLog
import pro.foundev.ingest.SchemaCreation

class CassandraRepositorySpec extends FunSpec with SparkSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach{

  var cassandraRepository: CassandraRepository = new CassandraRepository()
  var connector: CassandraConnector = _
  var session: Session = _

  def addLog(log: IpLog): Unit = {
    val id = log.id
    val state = log.originState
    val ip = log.ipAddress.toString.stripPrefix("/")
    val urls = log.urls.map(x=>"'"+x+"'").mkString(",")
     session.execute("INSERT INTO " + Configuration.getKeyspace + "." + Configuration.getTable +
         s" (id, origin_state, ip_address, urls) values ( $id, '$state', '$ip', [ $urls ] )"
     )
  }
  override def beforeAll(): Unit = {
    super.beforeAll()
    connector = CassandraConnector(conf)
    session = connector.openSession()
    new SchemaCreation(session).createSchema()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    session.execute("truncate %s.%s".format(Configuration.getKeyspace, Configuration.getTable))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    session.execute("truncate %s.%s".format(Configuration.getKeyspace, Configuration.getTable))
    session.close()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    session.execute("truncate %s.%s".format(Configuration.getKeyspace, Configuration.getTable))
  }

  describe("getting logs by ids"){
    def buildLogs(): Unit ={
      val ip = InetAddress.getByName("127.0.0.1")
      addLog(new IpLog(1, "TX", ip, List("http://foo.net", "http://bar.net")))
      addLog(new IpLog(2, "IN", ip, List("http://bar.net")))
      addLog(new IpLog(3, "MI", ip, List("http://bar.net")))
      addLog(new IpLog(4, "CA", ip, List("http://bar.net")))
    }
    describe("no rows"){
      it("should have no rows") {
        val logs = cassandraRepository.getLogsForIds(Set(1, 2, 3), connector)
        logs should have size 0
      }

    }
    describe("matching rows"){
      it("should find only the matching ids") {
        buildLogs()
        val logs = cassandraRepository.getLogsForIds(Set(1, 2, 3), connector)
        logs should have size 3
      }
    }
  }
  describe("getting logs by state"){
    describe("no rows"){
      it("should find no data") {
        val logs = cassandraRepository.getLogsFromRandom2iUsingConnector("TX", connector)
        logs should have size 0
      }
    }
    describe("matching rows"){
      it("should find rows that match state name") {
        val ip = InetAddress.getByName("127.0.0.1")
        addLog(new IpLog(1, "TX", ip, List("http://foo.net", "http://bar.net")))
        addLog(new IpLog(2, "TX", ip, List("http://bar.net")))
        addLog(new IpLog(3, "TX", ip, List("http://bar.net")))
        addLog(new IpLog(4, "CA", ip, List("http://bar.net")))
        val logs = cassandraRepository.getLogsFromRandom2iUsingConnector("TX", connector)
        logs should have size 3
        logs(0).originState should equal("TX")
        logs(1).originState should equal("TX")
        logs(2).originState should equal("TX")
      }
    }
  }
  describe("getting logs by state with spark job") {
    describe("matching rows") {
      it("should find rows that match state name") {
        val ip = InetAddress.getByName("127.0.0.1")
        addLog(new IpLog(1, "TX", ip, List("http://foo.net", "http://bar.net")))
        addLog(new IpLog(2, "TX", ip, List("http://bar.net")))
        addLog(new IpLog(3, "TX", ip, List("http://bar.net")))
        addLog(new IpLog(4, "CA", ip, List("http://bar.net")))
        val logs = cassandraRepository.getLogsFromRandom2i("TX", sc).collect()
        logs should have size 3
        logs(0).originState should equal("TX")
        logs(1).originState should equal("TX")
        logs(2).originState should equal("TX")
      }
    }
  }
}
