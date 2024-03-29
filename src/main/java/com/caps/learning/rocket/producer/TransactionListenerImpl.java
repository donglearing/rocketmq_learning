package com.caps.learning.rocket.producer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * Created by pengfei.dong Date 2019-12-05 Time 00:29
 */

public class TransactionListenerImpl implements TransactionListener {

  private AtomicInteger transactionIndex = new AtomicInteger(0);

  private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

  @Override
  public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
    int status = (Integer)arg % 3;
    localTrans.put(msg.getTransactionId(), status);
    System.out.println(status);
////    if(status == 2){
//      return LocalTransactionState.COMMIT_MESSAGE;
////    }
    return LocalTransactionState.UNKNOW;
  }

  @Override
  public LocalTransactionState checkLocalTransaction(MessageExt msg) {
    Integer status = localTrans.get(msg.getTransactionId());
    if (null != status) {
      switch (status) {
        case 0:
          return LocalTransactionState.UNKNOW;
        case 1:
          return LocalTransactionState.COMMIT_MESSAGE;
        case 2:
          return LocalTransactionState.ROLLBACK_MESSAGE;
      }
    }
    System.out.println("checkLocalTransaction excute");
    return LocalTransactionState.COMMIT_MESSAGE;
  }
}

