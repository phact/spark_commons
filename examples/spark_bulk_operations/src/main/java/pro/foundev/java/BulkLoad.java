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

package pro.foundev.java;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import pro.foundev.java.Transformations.CommaSplit;
import pro.foundev.java.Transformations.GetUninterruptedly;
import pro.foundev.java.Transformations.MapColumns;
import scala.Function1;
import scala.Tuple2;

public class BulkLoad extends AbstractCassandraConnectorClass {
    public static void main(String[] args){
        new BulkLoad().run();
    }
    @Override
    protected void run() {
        CassandraConnectionContext context = connectToCassandra();
        JavaSparkContext sc = context.getSparkContext();
        final CassandraConnector connector = context.getConnector();

        JavaRDD<String> rdd = sc.textFile("src/main/resources/example.txt");
        JavaRDD<String[]> columnArrays = rdd.map(new CommaSplit());
        JavaRDD<Tuple2<Integer,JsonNode>> userJson = columnArrays.map(new MapColumns());

        JavaRDD<ResultSetFuture> insertFutures = userJson.map(new Function<Tuple2<Integer, JsonNode>, ResultSetFuture>() {
            @Override
            public ResultSetFuture call(final Tuple2<Integer, JsonNode> node) throws Exception {
                try(Session session = connector.openSession()){
                    //the prepare is cached per node. While this would appear to be an anti-pattern preparing
                    // on every iteration this in fact will only prepare once per node
                    PreparedStatement insertStatement = session.prepare(
                            String.format("insert into %s ( id, value, version) values (?, ?, 1)"
                                    , getFullTableName()));
                    return session.executeAsync(insertStatement.bind(node._1(), node._2()));
                }
            }
        });
        insertFutures.foreach(new GetUninterruptedly());
    }
}
