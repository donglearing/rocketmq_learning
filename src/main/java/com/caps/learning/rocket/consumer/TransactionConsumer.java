package com.caps.learning.rocket.consumer;

import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * Created by pengfei.dong Date 2019-12-05 Time 00:25
 */

public class TransactionConsumer {
  public static void main(String[] args) throws InterruptedException, MQClientException {

    // Instantiate with specified consumer group name.
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_PAY_ACCOUNT");

    // Specify name server addresses.
    consumer.setNamesrvAddr("47.100.200.152:9876");

    // Subscribe one more more topics to consume.
    consumer.subscribe("PAY_ACCOUNT", "*");
    // Register callback to execute on arrival of messages fetched from brokers.
    consumer.registerMessageListener(new MessageListenerConcurrently() {

      @Override
      public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
          ConsumeConcurrentlyContext context) {
        for (MessageExt messageExt : msgs) {
          System.out.println(new String(messageExt.getBody()));
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
      }
    });

    //Launch the consumer instance.
    consumer.start();
    System.out.printf("Consumer Started.%n");
  }

}
