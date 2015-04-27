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


import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import pro.foundev.Args;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class SimpleStateHandling implements Serializable {

    private final String master;

    public SimpleStateHandling(String master) {
        this.master = master;
    }

    public static void main(String[] args){
        String master = Args.parseMaster(args);
        new SimpleStateHandling(master).startJob();
    }

    private void startJob() {
        final SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", master)
                .setMaster("spark://"+master+" :7077")
                .setAppName("Windowed_Rapid_Transaction_Check");
        JavaStreamingContext sc = new JavaStreamingContext(conf, new Duration(500));

        sc.checkpoint("cfs:///simple_state_handling_checkpoint");
        Queue<JavaRDD<Integer>> rddQueue = new LinkedList<>();
        JavaRDD<Integer> integerJavaRDD1 = sc.sparkContext().parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
        rddQueue.add(integerJavaRDD1);
        JavaRDD<Integer> integerJavaRDD2 = sc.sparkContext().parallelize(Arrays.asList(1, 2, 33, 44, 55, 66, 77, 88));
        rddQueue.add(integerJavaRDD2);
        JavaRDD<Integer> integerJavaRDD3 = sc.sparkContext().parallelize(Arrays.asList(1, 2));
        rddQueue.add(integerJavaRDD3);
        sc.queueStream(rddQueue)
        .mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<>(integer, 1);
            }
        }).updateStateByKey(new UpdateStateFunc()).print();
        sc.start();
        sc.awaitTermination();
    }
    class UpdateStateFunc implements Function2<List<Integer>, Optional<Integer>, Optional<Integer>>{

        @Override
        public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
            if (v1.size() == 0) {
                //remove numbers that are not in the last set, this is acting in part as a reduction function
                return Optional.absent();
            } else {
                if(v2.isPresent()) {


                    int count = 0;
                    for (Integer i : v1) {
                        count = +i;
                    }
                    Integer i = v2.get();
                    return Optional.of(i + count);
                }
                return v2;
            }
        }
    }
}
