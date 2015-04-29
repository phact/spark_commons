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

import play.api.libs.json._

object BulkLoad extends CassandraCapable {

  def main(args: Array[String]): Unit = {
    val context = connectToCassandra()
    val sc = context.sparkContext
    val connector = context.connector

    val rdd = sc.textFile("src/main/resources/example.txt")
    rdd.map(line => line.split(','))
      .map(array => {
      val id = array(0).toInt
      val body = Json.toJson(
        Map(
          "firstName" -> Json.toJson(array(1)),
          "lastName" -> Json.toJson(array(2))
        )).toString()
      connector.withSessionDo(session => {
        val insertStatement = session.prepare(s"insert into ${getFullTableName()} ( id, value, version) values (?, ?, 1)")
        session.executeAsync(
          insertStatement.bind(
            id: java.lang.Integer,
            body
          ))
      })
    }).foreach(x => x.getUninterruptibly())
  }
}
