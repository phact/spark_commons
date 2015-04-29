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
package pro.foundev.ingest;

import com.datastax.driver.core.*;
import com.github.javafaker.Faker;
import com.google.common.net.InetAddresses;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import static pro.foundev.Configuration.getKeyspace;
import static pro.foundev.Configuration.getTable;


public class Loader {

    private static Logger logger = Logger.getLogger(Loader.class.toString());
    private final String contactHost;
    private final Random random;

    public Loader(String contactHost) {

        this.contactHost = contactHost;
        this.random = new Random();
    }
    private String createIpAddress() {
        //silly and naive but quick and should only generate valid addresses
        int oct = 253;
        return String.format("%d.%d.%d.%d", random.nextInt(oct) +1, random.nextInt(oct) + 1,
                random.nextInt(oct)+1, random.nextInt(oct)+1);
    }
    public void create(long recordsToCreate) {

        Faker faker = new Faker();
        Cluster cluster = Cluster.builder().addContactPoint(contactHost).build();
        Session session = cluster.newSession();
        new SchemaCreation(session).createSchema();
        PreparedStatement st = session.prepare("INSERT INTO " + getKeyspace() +
                "." + getTable()  + " (id, origin_state, " +
                "ip_address, urls) values ( ?, ?, ?, ?)");
        long tickInterval = recordsToCreate/100;
        int ticksCompleted=0;
        for (long i = 0; i < recordsToCreate; i++) {
            List<ResultSetFuture> futures = new ArrayList<>();
            InetAddress fakeIp = InetAddresses.forString(createIpAddress());
            String fakeState = faker.address().stateAbbr();
            List<String> urls = new ArrayList<>();
            int seed = random.nextInt(10000);
            for (int j = 0; j < seed; j++) {
               urls.add(faker.internet().url());
            }
            BoundStatement bs = st.bind(i, fakeState, fakeIp, urls);
            //      setLong(0, i).
            //        setString(1, fakeState).
            //        setInet(2, fakeIp ).
            //        setList(3, urls);
            //
            futures.add(session.executeAsync(bs));
            if(i % 100==0){
                for (ResultSetFuture future:futures){
                    try {
                        future.getUninterruptibly(2, TimeUnit.SECONDS);
                    } catch (TimeoutException e) {
                        logger.warning(e.getMessage());
                        logger.warning("retrying");
                    }
                }
            }
            if(i!=0 && i % tickInterval==0){
                ticksCompleted ++;
                StringBuilder builder = new StringBuilder();
                for (int j = 0; j < ticksCompleted ; j++) {
                    builder.append("=");
                }
                long ticksLeft = 100 - ticksCompleted;
                if(ticksLeft<0){
                    throw new RuntimeException("ticks left is " + ticksLeft + " and I is " + i);
                }
                for (int j = 0; j < ticksLeft; j++) {
                   builder.append(" ");
                }

               System.out.println("["+ builder.toString() + "]\r");
            }
        }
    }
}
