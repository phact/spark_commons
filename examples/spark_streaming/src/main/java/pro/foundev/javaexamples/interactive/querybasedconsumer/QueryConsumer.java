package pro.foundev.javaexamples.interactive.querybasedconsumer;

import com.datastax.driver.core.*;
import com.rabbitmq.client.*;
import pro.foundev.javaexamples.Warning;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

public class QueryConsumer {
    public static void main(String[] args){
        new QueryConsumer().run();
    }
    private static final String EXCHANGE_NAME = "warnings_response";
    public void run(){
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .build();
        final Session session = cluster.connect();
        session.execute(String.format("CREATE TABLE IF NOT EXISTS tester.warningsrdd (ssn text, " +
                "batchStartTime bigint, id uuid, amount decimal, rule text, PRIMARY KEY(batchStartTime, id))"));
        final PreparedStatement preparedStatement = session.prepare("SELECT * FROM tester.warningsrdd where batchStartTime = ?");
        try {
            Connection connection = factory.newConnection();
            final Channel channel = connection.createChannel();
            final String queue = channel.queueDeclare().getQueue();
            channel.queueBind(queue, EXCHANGE_NAME, "");

            final Consumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body);
                    long batchStartTime = Long.parseLong(message);
                    System.out.println("Writing batch with start time of " + new Date(batchStartTime));
                    ResultSet warningsResultSet = session.execute(preparedStatement.bind(batchStartTime));
                    int count = 0;
                    for (Row warning : warningsResultSet) {
                        count +=1;
                        BigDecimal decimal = warning.getDecimal("amount");
                        UUID id = warning.getUUID("id");
                        String ssn = warning.getString("ssn");
                        String rule = warning.getString("rule");
                        Warning warningObj = new Warning();
                        warningObj.setAmount(decimal);
                        warningObj.setId(id);
                        warningObj.setSsn(ssn);
                        warningObj.setRule(rule);
                        notifyUI(warningObj);
                    }
                    System.out.println("Batch with start time of " + new Date(batchStartTime) + " Complete with " + count + " items.");
                }
            };
            channel.basicConsume(queue, true, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void notifyUI(Warning warningObj){
        System.out.println(warningObj);
        //TODO: do remote call to UI, example webhook via http post
    }


}
