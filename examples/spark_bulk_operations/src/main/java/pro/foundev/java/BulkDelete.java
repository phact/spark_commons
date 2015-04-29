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

package pro.foundev.java;


import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.api.java.function.Function;
import pro.foundev.java.Transformations.GetUninterruptedly;

public class BulkDelete extends AbstractCassandraConnectorClass {

    public static void main(String[] args){
        new BulkDelete().run();
    }

    @Override
    protected void run() {
        CassandraConnectionContext context = connectToCassandra();
        CassandraJavaRDD<CassandraRow> rdd = context.getRdd();
        final CassandraConnector connector = context.getConnector();
        rdd.where("version=1").map(new Function<CassandraRow, ResultSetFuture>() {
            @Override
            public ResultSetFuture call(final CassandraRow row) throws Exception {
                try (Session session = connector.openSession()) {
                    PreparedStatement deleteStatement = session.prepare(
                            String.format(
                                    "delete from %s where id = ? and version = 1",
                                    getFullTableName()));
                    int id = row.getInt(0);
                    return session.executeAsync(deleteStatement.bind(id));
                }
            }
        }).foreach(new GetUninterruptedly());
    }
}


