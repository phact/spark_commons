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

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CassandraConnectionContext {

    private CassandraConnector connector;
    private CassandraJavaRDD<CassandraRow> rdd;
    private JavaSparkContext sparkContext;

    public CassandraConnectionContext(CassandraConnector connector,
                                      CassandraJavaRDD<CassandraRow> rdd,
                                      JavaSparkContext sparkContext) {
        this.connector = connector;
        this.rdd = rdd;
        this.sparkContext = sparkContext;
    }

    public CassandraConnector getConnector() {
        return connector;
    }

    public void setConnector(CassandraConnector connector) {
        this.connector = connector;
    }

    public CassandraJavaRDD<CassandraRow> getRdd() {
        return rdd;
    }

    public void setRdd(CassandraJavaRDD<CassandraRow> rdd) {
        this.rdd = rdd;
    }

    public JavaSparkContext getSparkContext() {
        return sparkContext;
    }

    public void setSparkContext(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }
}
