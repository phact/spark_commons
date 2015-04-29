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

object SchemaMigration extends CassandraCapable {

  def getFullMigratedTable(): String = {
    s"${getKeySpaceName()}.kvWithMarketId"
  }

  def getMarket(id: java.lang.Integer): String = id match {
    case id if 0 until 200 contains id => "US"
    case id if 201 until 400 contains id => "UK"
    case id if 401 until 600 contains id => "AU"
    case id if 601 until 800 contains id => "CN"
    case id if 801 until 1000 contains id => "FR"
    case _ => "Unknown"
  }

  def main(args: Array[String]): Unit = {
    val context = connectToCassandra()
    val connector = context.connector;
    connector.withSessionDo(session => {
      session.execute(s"create table if not exists ${getFullMigratedTable()} " +
        "(marketId text, id int, version int, value text, PRIMARY KEY((marketId, id), version)" +
        ")")
    })

    context.rdd.map(row => {
      val id: java.lang.Integer = row.getInt("id")
      val version: java.lang.Integer = row.getInt("version")
      val value = row.getString("value")
      val marketId = getMarket(id)
      connector.withSessionDo(session => {
        val statement = session.prepare(s"INSERT INTO ${getFullMigratedTable()} (marketId, id, version, value) " +
          "values (?, ?, ?, ?)")
        val bound = statement.bind(marketId, id, version, value)
        session.executeAsync(bound)
      })
    }).foreach(x => x.getUninterruptibly())
  }
}
