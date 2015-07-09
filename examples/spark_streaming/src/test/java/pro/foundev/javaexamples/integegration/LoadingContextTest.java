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

package pro.foundev.javaexamples.integegration;

import com.datastax.bdp.spark.DseSparkConfHelper;
import com.datastax.bdp.spark.DseSparkContext;
import com.datastax.bdp.spark.DseStreamingContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * executable example of a working spark test.
 * Very simplistic butit gets the job done
 * TODO move me to sample worksheet
 * */
public class LoadingContextTest implements Serializable{
    private transient JavaStreamingContext ssc;
    private List<String> results;

    @Before
    public void setUp() {
        results = new ArrayList<>();
        SparkConf conf = new SparkConf()
                .setAppName("test_app")
                .set("spark.cassandra.connection.host", "127.0.0.1")
                .setMaster("local[2]");
               // .set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock");
        SparkContext ctx = DseSparkContext.apply(conf); //relies on dse-4.7.0.jar included in dse-4.7.0/lib
        ssc = new JavaStreamingContext(new JavaSparkContext(ctx), new Duration(500));
        ssc.checkpoint("checkpoint");
    }

    @After
    public void tearDown() {
        if (ssc != null) {
            ssc.stop();
        }
    }

    private JavaRDD<Integer> create(Integer... vargs){
        return ssc.sparkContext().parallelize(Arrays.asList(vargs));
    }

    @Test
    public void itInitializesSpark() throws InterruptedException {

        Queue<JavaRDD<Integer>> rddQueue = new LinkedList<>();
        Function2<JavaPairRDD<Integer, Integer>, Time, Void> func = new Function2<JavaPairRDD<Integer, Integer>, Time, Void>() {
            @Override
            public Void call(JavaPairRDD<Integer, Integer> v1, Time v2) throws Exception {
                List<Tuple2<Integer, Integer>> tuple2s = v1.collect();
                for( Tuple2<Integer, Integer> t: tuple2s){
                   results.add(t._1()+":"+t._2());
                }
                return null;
            }
        };
        rddQueue.add(create(1, 1, 1, 3, 4));
        ssc.queueStream(rddQueue).mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<>(integer, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }).foreachRDD(func);
        ssc.start();
        Thread.sleep(2000);
        Collections.sort(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals("1:3", results.get(0));
        Assert.assertEquals("3:1", results.get(1));
        Assert.assertEquals("4:1", results.get(2));
    }

}
