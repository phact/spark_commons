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

package pro.foundev.examples.spark_streaming.java;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import pro.foundev.examples.spark_streaming.java.cassandra.CassandraConnectionContext;
import pro.foundev.examples.spark_streaming.java.dto.Transaction;
import pro.foundev.examples.spark_streaming.java.dto.Warning;
import pro.foundev.examples.spark_streaming.java.messaging.AbstractRabbitMQConnector;
import scala.Tuple2;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.*;



import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class WindowedCalculationsAndEventTriggering extends AbstractRabbitMQConnector {
    protected WindowedCalculationsAndEventTriggering(String[] args) {
        super(args, "cfs:///windowed_calculations_checkpoint");
    }

    public static void main(String[] args) {
        new WindowedCalculationsAndEventTriggering(args).startJob();
    }

    public JavaStreamingContext createContext() {
        CassandraConnectionContext connectionContext = connectToCassandra();
        JavaDStream<String> rdd = connectionContext.getRDD();
        JavaStreamingContext sparkContext = connectionContext.getStreamingContext();
        final CassandraConnector connector = connectionContext.getCassandraConnector();

        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        JavaDStream<Tuple2<String,Transaction>> transactionList
                = rdd.map(new
                                  Function<String,Tuple2<String,Transaction>>() {
                                      @Override
                                      public Tuple2<String, Transaction> call(String line) throws Exception {
                                          String[] columns = line.split(",");
                                          String taxId = columns[0];
                                          String name = columns[1];
                                          String merchant = columns[2];
                                          BigDecimal amount = new BigDecimal(columns[3]);
                                          Date transactionDate;
                                          try {
                                              transactionDate = format.parse(columns[4]);
                                          } catch (ParseException e) {
                                              e.printStackTrace();
                                              throw new RuntimeException(e);
                                          }
                                          String tranId = columns[5];
                                          System.out.println(line);
                                          Tuple2<String, Transaction> transaction = new Tuple2<>(taxId,
                                                  new Transaction(name, merchant, amount, transactionDate, tranId));
                                          return transaction;
                                      }
                                  }
        );
        transactionList.cache();

        final String warningsTableName = "warnings";
        try(Session session = connector.openSession()) {
            session.execute(String.format("DROP TABLE IF EXISTS %s.%s",
                    getKeySpaceName(), warningsTableName));
            session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (ssn text, " +
                    "id uuid, amount decimal, rule text, PRIMARY KEY(ssn, id))",getKeySpaceName(), warningsTableName));
        }

        //setup warning on more than certain number of transactions by user in a 60 second window, every 10 seconds
        JavaPairDStream<String, BigDecimal> warnings = transactionList.window(new Duration(60000), new Duration(10000))
                .mapToPair(new PairFunction<Tuple2<String, Transaction>, String, BigDecimal>() {
                    @Override
                    public Tuple2<String, BigDecimal> call(Tuple2<String, Transaction> transaction) throws Exception {
                        String taxId = transaction._1();
                        BigDecimal amount = transaction._2().getAmount();
                        return new Tuple2<>(taxId, amount);
                    }
                })
                .reduceByKey(new Function2<BigDecimal, BigDecimal, BigDecimal>() {
                    @Override
                    public BigDecimal call(BigDecimal transactionAmount1, BigDecimal transactionAmount2) throws Exception {
                        return transactionAmount1.add(transactionAmount2);
                    }
                }).filter(new Function<Tuple2<String, BigDecimal>, Boolean>() {
                              @Override
                              public Boolean call(Tuple2<String, BigDecimal> transactionSumByTaxId) throws Exception {
                                  //returns true if total is greater than 999
                                  Boolean result = transactionSumByTaxId._2().compareTo(new BigDecimal(999)) == 1;
                                  System.out.println("tran " + transactionSumByTaxId._1() + " with amount " + transactionSumByTaxId._2());
                                  if (result) {
                                      System.out.println("warning " + transactionSumByTaxId._1() + " has value " + transactionSumByTaxId._2() + " is greater than 999");
                                  }
                                  return result;
                              }
                          }
                );
        JavaDStream<Warning> mappedWarnings = warnings.map(new Function<Tuple2<String, BigDecimal>, Warning>() {
            @Override
            public Warning call(Tuple2<String, BigDecimal> warnings) throws Exception {
                Warning warning = new Warning();
                warning.setSsn(warnings._1());
                warning.setId(UUIDs.timeBased());
                warning.setAmount(warnings._2());
                warning.setRule("OVER_DOLLAR_AMOUNT");
                return warning;
            }
        });
        javaFunctions(mappedWarnings).writerBuilder(getKeySpaceName(), warningsTableName,mapToRow(Warning.class))
                .saveToCassandra();
        return sparkContext;
    }

}
