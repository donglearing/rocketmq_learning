package com.caps.learning.rocket.producer;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * Created by pengfei.dong Date 2019-12-05 Time 00:30
 */

public class TransactionProducerTest {
  public static void main(String[] args) throws MQClientException, InterruptedException {
    TransactionListener transactionListener = new TransactionListenerImpl();
    TransactionMQProducer producer = new TransactionMQProducer("CID_PAY_ACCOUNT");
    producer.setNamesrvAddr("47.100.200.152:9876");

    ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000),
        new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName("client-transaction-msg-check-thread");
        return thread;
      }
    });

    producer.setExecutorService(executorService);
    producer.setTransactionListener(transactionListener);
    producer.start();

    String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};

    try {
      Map<String, String> paramMap = new HashMap<>();
      paramMap.put("type", "6");
      paramMap.put("bizOrderId", "15414012438257823");
      paramMap.put("payOrderId", "15414012438257823");
      paramMap.put("amount", "10");
      paramMap.put("userId", "200001");
      paramMap.put("tradeType", "charge");
      paramMap.put("financeStatus", "0");//财务状态，应收
      paramMap.put("channel", "a");//余额
      paramMap.put("tradeTime", "20190101202022");
      paramMap.put("nonce_str", "xkdkskskdksk");


      //拼凑消息体
      for(int index = 0; index < 20; index ++){
        paramMap.put("userId", "200001_dongpf" + index);
        paramMap.put("index", String.valueOf(index));
        Message msg = new Message("TEST_TRANSACTION", "TagA",paramMap.toString().getBytes(RemotingHelper.DEFAULT_CHARSET));
        SendResult sendResult = producer.sendMessageInTransaction(msg, index);
        System.out.printf("%s%n", sendResult);
        Thread.sleep(100);

      }


    } catch (MQClientException | UnsupportedEncodingException e) {
      e.printStackTrace();
    }

    Thread.sleep(10*1000);
//    producer.shutdown();
  }
}

