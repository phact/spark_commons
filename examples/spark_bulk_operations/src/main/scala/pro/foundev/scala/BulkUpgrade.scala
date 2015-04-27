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

object BulkUpgrade extends CassandraCapable {

  def upgradeSchema(oldJson: String): String = {
    val user = Json.parse(oldJson)
    val firstName = user.\("firstName")
    val lastName = user.\("lastName")
    val fullName = firstName + " " + lastName
    val json = Json.toJson(Map(
      "firstName" -> Json.toJson(firstName),
      "lastName" -> Json.toJson(lastName),
      "fullName" -> Json.toJson(fullName)))
    json.toString()
  }

  def main(args: Array[String]): Unit = {
    val context = connectToCassandra()
    val rdd = context.rdd
    val connector = context.connector

    rdd.where("version=1").cache().map(
      x => {
        val id = x.get[Object]("id")
        val value = x.get[String]("value")
        val converted = upgradeSchema(value)
        connector.withSessionDo(session => {
          val updateStatement = session.prepare(s"update ${getFullTableName()} set value = ? where id = ? and version  = 2")
          val bound = updateStatement.bind(converted, id)
          session.executeAsync(bound)
        })
      }).foreach(x => x.getUninterruptibly())
  }
}
