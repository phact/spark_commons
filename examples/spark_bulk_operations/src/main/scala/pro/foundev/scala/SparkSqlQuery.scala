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

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlQuery extends CassandraCapable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .setJars(Array("target/scala-2.10/spark_bulk_ops-assembly-0.2.0.jar"))
    val sc = new SparkContext("spark://127.0.0.1:7077", "test", conf)
    val connector = CassandraConnector(conf)

    connector.withSessionDo(session => {
      session.execute("create keyspace if not exists activity_stream_api with replication = { 'class':'SimpleStrategy', " +
        "'replication_factor':1 }")
      session.execute("create table if not exists activity_stream_api.tag_activity " +
        "( activity_id int, tag text, PRIMARY KEY(activity_id, tag) )")
      session.execute("create table if not exists activity_stream_api.activity " +
        "( activity_id int, activity text, PRIMARY KEY(activity_id) )")
      session.execute("truncate activity_stream_api.activity")
      session.execute("truncate activity_stream_api.tag_activity")
      session.execute("insert into activity_stream_api.activity " + "( activity_id, activity) values ( 1, 'inserted' )")
      session.execute("insert into activity_stream_api.activity " + "( activity_id, activity) values ( 2, 'updated' )")
      session.execute("insert into activity_stream_api.activity " + "( activity_id, activity) values ( 3, 'deleted' )")
      session.execute("insert into activity_stream_api.tag_activity " + "( activity_id, tag) values ( 1, 'us' )")
      session.execute("insert into activity_stream_api.tag_activity " + "( activity_id, tag) values ( 2, 'us' )")
      session.execute("insert into activity_stream_api.tag_activity " + "( activity_id, tag) values ( 3, 'uk' )")
    })

    val cassandraContext = new CassandraSQLContext(sc)
    val rdd = cassandraContext.sql("select t.tag, count(*) as cnt from activity_stream_api.activity a " +
      "join activity_stream_api.tag_activity t on t.activity_id = a.activity_id group by t.tag order by cnt");

    rdd.collect().foreach(f => println(f))
  }
}

