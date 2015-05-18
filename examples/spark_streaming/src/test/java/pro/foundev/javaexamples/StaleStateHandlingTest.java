
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

package pro.foundev.javaexamples;

import com.datastax.bdp.spark.DseSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class StaleStateHandlingTest implements Serializable{

    private transient JavaStreamingContext ssc;

    @Before
    public void setUp(){

        SparkConf conf =new SparkConf()
                .setAppName("test_app")
                .setMaster("local[*]")
                .set("spark.streaming.clock", "org.apache.spark.util.ManualClock");
        SparkContext sparkContext = DseSparkContext.apply(conf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);
        ssc = new JavaStreamingContext(javaSparkContext, new Duration(1000));
        ssc.checkpoint("checkpoint");
    }

    @After
    public void tearDown(){
        if(ssc!=null){
            ssc.stop();
        }
    }

    @Test
    @Ignore
    public void itRemovesStaleData(){

    }

    @Test
    @Ignore
    public void itIncludesRecordsFromPreviousBatchEvenIfNoNewInformationForThem(){

    }

    @Test
    @Ignore
    public void itAccumulatesTotalsBetweenBatches(){

    }
}
