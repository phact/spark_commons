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

import com.datastax.spark.connector.japi.CassandraRow;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.api.java.function.Function;
import play.libs.Json;

public class BulkExport extends AbstractCassandraConnectorClass {

    public static void main(String[] args){
        new BulkExport().run();
    }

    @Override
    protected void run() {
        connectToCassandra().getRdd().map(
                new Function<CassandraRow, String>() {
                    @Override
                    public String call(CassandraRow row) throws Exception {
                        String body = row.getString("value");
                        JsonNode jsonBody = Json.parse(body);
                        String firstName = jsonBody.findPath("firstName").textValue();
                        String lastName = jsonBody.findPath("lastName").textValue();
                        return String.format("%i,%s,%s",
                                row.getInt("id"),
                                firstName,
                                lastName);
                    }
                }
        ).saveAsTextFile("/tmp/bulk_output/");
    }
}
