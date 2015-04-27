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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class StaleStateHandling extends AbstractRabbitMQConnector{

    protected StaleStateHandling(String[] args) {
        super(args);
    }

    public static void main(String[] args){
        new StaleStateHandling(args).startJob();
    }

    @Override
    protected JavaStreamingContext createContext() {
        CassandraConnectionContext connectionContext = connectToCassandra();
        JavaDStream<String> rdd = connectionContext.getRDD();
        JavaStreamingContext sparkContext = connectionContext.getStreamingContext();

        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        JavaDStream<Tuple2<String,Transaction>> transactionList
                = rdd.map(new
                                  Function<String, Tuple2<String, Transaction>>() {
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
        transactionList.mapToPair(new PairFunction<Tuple2<String, Transaction>, String, TransactionTotal>() {
            @Override
            public Tuple2<String, TransactionTotal> call(Tuple2<String, Transaction> stringTransactionTuple2) throws Exception {
                String ssn = stringTransactionTuple2._1();
                BigDecimal amount = stringTransactionTuple2._2().getAmount();
                TransactionTotal total = new TransactionTotal();
                total.setAmount(amount);
                total.setCount(1);
                total.setMax(amount);
                total.setLastTransaction(stringTransactionTuple2._2().getTransactionDate());
                return new Tuple2<>(ssn, total);
            }
        }).reduceByKey(new Function2<TransactionTotal, TransactionTotal, TransactionTotal>() {
            @Override
            public TransactionTotal call(TransactionTotal v1, TransactionTotal v2) throws Exception {
                return v1.add(v2);
            }
        }).updateStateByKey(new Function2<List<TransactionTotal>, Optional<TransactionTotal>, Optional<TransactionTotal>>() {
            @Override
            public Optional<TransactionTotal> call(List<TransactionTotal> totals, Optional<TransactionTotal> optional) throws Exception {
                ///nothing in this current microbatch for this transactiontotal
                TransactionTotal total = optional.get();
                if (totals.size() == 0) {
                    Calendar twoDaysAgo = Calendar.getInstance();
                    twoDaysAgo.add(Calendar.DATE, -2);
                    //if last transactiondate is over 2 days old
                    if(twoDaysAgo.after(total.getLastTransaction())){
                        //remove data
                        return Optional.absent();
                    }
                    //since nothing else to do go ahead and return original
                    return Optional.of(total);
                }
                //default empty total copy from total
                TransactionTotal newTotal = total.add(new TransactionTotal());

                //since we've found new data keep adding it to existing total
                for (TransactionTotal t:totals){
                    newTotal = t.add(newTotal);
                }
                //return new stateful amount
                return Optional.of(newTotal);
            }
        });
        return sparkContext;

    }
}
