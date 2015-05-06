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
package pro.foundev.examples.spark_streaming.java.messaging;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import pro.foundev.examples.spark_streaming.utils.Args;
import pro.foundev.examples.spark_streaming.java.cassandra.CassandraConnectionContext;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public abstract class AbstractRabbitMQConnector implements Serializable {

    private final String keySpaceName;
    private final String primaryTableName;
    protected final String checkpointDir;
    protected final String master;

    public AbstractRabbitMQConnector(String[] args, String checkpointDir) {
        this(Args.parseMaster(args), checkpointDir);
    }

    protected String getPrimaryTableName() { return primaryTableName; }
    protected String getKeySpaceName() { return keySpaceName; }
    protected String getFullTableName(){ return String.format("%s.%s",getKeySpaceName(), getPrimaryTableName()); }

    protected AbstractRabbitMQConnector(String master, String checkpointDir){
        this.master = master;
        this.checkpointDir = checkpointDir;
        this.keySpaceName = "tester";
        this.primaryTableName = "streaming_demo";
    }

    protected CassandraConnectionContext connectToCassandra(){
        final SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", master)
                .setMaster("spark://" + master + ":7077")
                .setAppName("Windowed_Rapid_Transaction_Check");
        JavaStreamingContext sc = new JavaStreamingContext(conf, new Duration(500));
        sc.checkpoint(checkpointDir);
        CassandraConnector connector = CassandraConnector.apply(conf);
        try (Session session = connector.openSession()) {
            session.execute(String.format("create keyspace if not exists %s with replication = { 'class':'SimpleStrategy', " +
                    "'replication_factor':1}", getKeySpaceName()));
            session.execute(String.format("drop table if exists %s",
                    getFullTableName()));
            session.execute(String.format("create table if not exists %s " +
                    "(userId int, userName text, followers Set<text>, PRIMARY KEY(userId))",
                    getFullTableName()));
            PreparedStatement preparedStatement = session.prepare(
                    String.format("INSERT INTO %s (userId, userName, " +
                    "followers) values (?,?,?)",
                    getFullTableName())
            );
            Set followers1 = new HashSet();
            followers1.add("jsmith");
            followers1.add("mark");
            followers1.add("mike");
            session.execute(preparedStatement.bind(0, "jsmith", followers1));
            Set followers2 = new HashSet();
            followers2.add("mark");
            followers2.add("mike");
            session.execute(preparedStatement.bind(1, "mark", followers2));
            Set followers3 = new HashSet();
            followers3.add("jsmith");
            followers3.add("mike");
            session.execute(preparedStatement.bind(2, "mike", followers3));
        }
        SparkContextJavaFunctions javaFunc = javaFunctions(sc);
        CassandraJavaRDD<CassandraRow> rdd = javaFunc
                .cassandraTable(getKeySpaceName(), getPrimaryTableName());
         JavaDStream<String> customReceiverStream = sc
                .receiverStream(new RabbitMQReceiver(StorageLevel.MEMORY_AND_DISK_2(), master));
        return new CassandraConnectionContext(connector, sc,customReceiverStream ,rdd);
    }
    protected abstract JavaStreamingContext createContext();

    public void startJob(){

        JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
            @Override
            public JavaStreamingContext create() {
                return createContext();
            }
        };
        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDir, factory);
        ssc.start();
        ssc.awaitTermination();
    }

}
