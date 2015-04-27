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
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.api.java.function.Function;

import java.util.List;

public class FindWithArgs extends AbstractCassandraConnectorClass {

    private final String[] args;

    public static void main(String[] args){
        new FindWithArgs(args).run();
    }

    public FindWithArgs(String[] args) {

        this.args = args;
    }

    @Override
    protected void run() {
        CassandraConnectionContext context = connectToCassandra();
        CassandraJavaRDD<CassandraRow> rdd = context.getRdd();
        String version = "1";
        if(args.length>0) {
            version = args[0];
        }
        System.out.println("reading version: " + version);
        List<String> rows = rdd.where("version=" + version).map(
                new Function<CassandraRow, String>() {
                    @Override
                    public String call(CassandraRow row) throws Exception {
                        return row.getString("value");
                    }
                }
        ).collect();
        for (String row: rows){
           System.out.println(row);
        }
    }
}
