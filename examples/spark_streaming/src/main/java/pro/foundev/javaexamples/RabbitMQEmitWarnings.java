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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import pro.foundev.Args;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Random;
import java.util.UUID;

public class RabbitMQEmitWarnings {
    private static final String EXCHANGE_NAME = "warnings";
    private static Thread mainThread;

    public static void main(String[] argv)
            throws java.io.IOException {
        String master = Args.parseMaster(argv);

        mainThread = Thread.currentThread();


        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(master);
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            public void run()
            {
                RabbitMQEmitWarnings.mainThread.interrupt();
                try {
                    channel.close();
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        while(true) {
            String message = buildMessage();
            channel.basicPublish(EXCHANGE_NAME, "", null,message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
    private static String buildMessage(){
        SecureRandom random = new SecureRandom();
        String ssn = String.format("%d%d%d-%d%d-%d%d%d%d",
                random.nextInt(9),
                random.nextInt(9),
                random.nextInt(9),
                random.nextInt(9),
                random.nextInt(9),
                random.nextInt(9),
                random.nextInt(9),
                random.nextInt(9),
                random.nextInt(9)
                );
        String name = String.format("Jim Jeffries");
        String merchant = String.format("Amazon");
        String amount = String.format("%d.%d%d",
                random.nextInt(10000),
                random.nextInt(9),
                random.nextInt(9)
                );
        String date = String.format("201%d-%02d-%02d",
                random.nextInt(5),
                random.nextInt(11)+1,
                random.nextInt(27)+1);
        String tranId = String.valueOf(UUID.randomUUID());
        String message = ssn + "," + name + "," +merchant + "," +amount + "," + date + "," + tranId;
        return message;
    }
}
