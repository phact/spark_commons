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

object BulkExport extends CassandraCapable {
  def main(args: Array[String]): Unit = {
    connectToCassandra().rdd.map(row => {
      val body = row.getString("value")
      val jsonBody = Json.parse(body)
      val firstName = jsonBody \ "firstName"
      val lastName = jsonBody \ "lastName"
      s"""${row.getInt("id")},${firstName},${lastName}"""

    }).saveAsTextFile("/tmp/bulk_output/")
  }
}
