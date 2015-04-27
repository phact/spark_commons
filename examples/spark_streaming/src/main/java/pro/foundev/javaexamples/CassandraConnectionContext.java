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
package pro.foundev.javaexamples;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.Serializable;

public class CassandraConnectionContext implements Serializable{
    private final CassandraConnector cassandraConnector;
    private final JavaStreamingContext streamingContext;
    private final JavaDStream<String> RDD;
    private final CassandraJavaRDD<CassandraRow> cassandraTableRDD;

    public CassandraConnectionContext(CassandraConnector cassandraConnector, JavaStreamingContext streamingContext, JavaDStream<String> RDD, CassandraJavaRDD<CassandraRow> cassandraTableRDD) {
        this.cassandraConnector = cassandraConnector;
        this.streamingContext = streamingContext;
        this.RDD = RDD;
        this.cassandraTableRDD = cassandraTableRDD;
    }


    public CassandraConnector getCassandraConnector() {
        return cassandraConnector;
    }

    public JavaStreamingContext getStreamingContext() {
        return streamingContext;
    }


    public JavaDStream<String> getRDD() {
        return RDD;
    }

    public CassandraJavaRDD<CassandraRow> getCassandraTableRDD() {
        return cassandraTableRDD;
    }
}
