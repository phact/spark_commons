package pro.foundev.messaging;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

abstract public class AbstractQueueReceiver extends Receiver<String> {

    public AbstractQueueReceiver(StorageLevel storageLevel) {
        super(storageLevel);
    }

    public abstract void setHost(String host);
    public abstract void setQueueName(String queueName);
}
