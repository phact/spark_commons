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

package pro.foundev;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class App {
    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setAppName( "My application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        CassandraConnector connector = CassandraConnector.apply(sc.getConf());

        try(Session session = connector.openSession() ){
            session.execute("CREATE KEYSPACE IF NOT EXISTS my_keyspace with replication = { 'class': 'SimpleStrategy'," +
                    " 'replication_factor': 1}");
            session.execute("CREATE TABLE IF NOT EXISTS my_keyspace.my_table (id int,first_name text, last_name text," +
                    " primary key(id))");
            session.execute("INSERT INTO my_keyspace.my_table (id, first_name, last_name) values (1, 'John', 'Smith')");
            session.execute("INSERT INTO my_keyspace.my_table (id, first_name, last_name) values (2, 'Mike', 'Jacobs')");
            session.execute("INSERT INTO my_keyspace.my_table (id, first_name, last_name) values (3, 'Mable', 'Adams')");
            session.execute("INSERT INTO my_keyspace.my_table (id, first_name, last_name) values (4, 'Jorge', 'Silva')");
        }
        JavaRDD<String> stringRdd = CassandraJavaUtil.javaFunctions(sc)
                .cassandraTable("my_keyspace", "my_table", mapColumnTo(String.class))
                .select("last_name");

        System.out.println("Printing out last names");
        for(String s: stringRdd.collect()){
            System.out.println(s);
        }

        //Mapping Cassandra column data to Java types¶
        JavaRDD<Integer> cassandraRddInt = CassandraJavaUtil.javaFunctions(sc)
                .cassandraTable("my_keyspace", "my_table", mapColumnTo(Integer.class))
                .select("id");
        System.out.println("Printing out person ids");
        for(Integer i: cassandraRddInt.collect()){
            System.out.println(i);
        }

        JavaRDD<Person> personRdd = CassandraJavaUtil.javaFunctions(sc)
                .cassandraTable("my_keyspace", "my_table", mapRowTo(Person.class));
        System.out.println("Printing out person records");
        for (Person person: personRdd.collect()){
            System.out.println(person);
        }

        CassandraJavaPairRDD<Integer, String> pairRdd = CassandraJavaUtil.javaFunctions(sc)
                .cassandraTable("my_keyspace", "my_table", mapColumnTo(Integer.class), mapColumnTo(String.class)).
        select("id", "first_name");
        for(Tuple2<Integer,String> pair:  pairRdd.collect()){
           System.out.println(String.format("Key: %d, Value: %s",pair._1(), pair._2()));
        }

        //Saving data to Cassandra¶
        System.out.println("saving updated data to cassandra");
        personRdd.foreach(new VoidFunction<Person>() {
            @Override
            public void call(Person person) throws Exception {
                person.setFirstName("Jason");
            }
        });
        CassandraJavaUtil.javaFunctions(personRdd)
                .writerBuilder("my_keyspace", "my_table", mapToRow(Person.class)).saveToCassandra();
    }
}
