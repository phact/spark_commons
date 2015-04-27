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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import org.apache.spark.api.java.function.Function;
import pro.foundev.java.Transformations.GetUninterruptedly;


public class SchemaMigration extends
        AbstractCassandraConnectorClass {

    public static void main(String[] args){
        new SchemaMigration().run();
    }

    private String getFullMigratedTable() {
        return String.format("%s.kvWithMarketId", getKeySpaceName());
    }
    private String getMarket(Integer id) {
        if (id >= 0 && id < 201) {
            return "US";
        } else if (id >= 201 && id < 401) {
            return "UK";
        } else if (id >= 401 && id < 601) {
            return "AU";
        } else if (id >= 601 && id < 801) {
            return "CN";
        } else if (id >= 801 && id < 1001) {
            return "FR";
        }
        return "Unknown";
    }

    @Override
    protected void run() {
        CassandraConnectionContext context = connectToCassandra();
        final CassandraConnector connector = context.getConnector();
        try(Session session = connector.openSession()) {
            session.execute(String.format("create table if not exists %s " +
                    "(marketId text, id int, version int, value text, PRIMARY KEY((marketId, id), version)" +
                    ")", getFullMigratedTable()));
        }

        context.getRdd().map(
                new Function<CassandraRow, ResultSetFuture>() {
                    @Override
                    public ResultSetFuture call(final CassandraRow row) throws Exception {
                        try (Session session = connector.openSession()) {

                            Integer id = row.getInt("id");
                            Integer version = row.getInt("version");
                            String value = row.getString("value");
                            String marketId = getMarket(id);

                            PreparedStatement statement = session.prepare(
                                    String.format(
                                            "INSERT INTO %s$ (marketId, id, version, value) " +
                                                    "values (?, ?, ?, ?)", getFullMigratedTable()));

                            BoundStatement bound = statement.bind(marketId, id, version, value);
                            return session.executeAsync(bound);
                        }
                    }
                }
        ).foreach(new GetUninterruptedly());
    }
}

