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

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;


public abstract class AbstractCassandraConnectorClass implements Serializable {

    private final String keySpaceName;
    private final String primaryTableName;

    protected String getPrimaryTableName() { return primaryTableName; }
    protected String getKeySpaceName() { return keySpaceName; }
    protected String getFullTableName(){ return String.format("%s.%s",getKeySpaceName(), getPrimaryTableName()); }

    protected AbstractCassandraConnectorClass(){
        keySpaceName = "spark_bulk_ops";
        primaryTableName = "kv";
    }

    protected CassandraConnectionContext connectToCassandra(){
        SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", "127.0.0.1");
        JavaSparkContext sc = new JavaSparkContext("spark://127.0.0.1:7077", "test", conf);
        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
        try (Session session = connector.openSession()) {
          session.execute(String.format("create keyspace if not exists %s with " +
                                                "replication = { 'class':'SimpleStrategy', " +
                        "'replication_factor':1}", getKeySpaceName()));
                                        session.execute(String.format("create table if not exists %s " +
                        "(marketId text, id int, version int, value text, PRIMARY KEY((id), version))"
                                        ,getFullTableName()));
        }
        SparkContextJavaFunctions javaFunc = CassandraJavaUtil.javaFunctions(sc);
        CassandraJavaRDD<CassandraRow> rdd = javaFunc
                .cassandraTable(getKeySpaceName(), getPrimaryTableName());
        return new CassandraConnectionContext(connector, rdd, sc);
    }
    protected abstract void run();
}
