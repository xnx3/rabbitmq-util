package com.xnx3.rabbitmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.xnx3.DateUtil;

public class RevDemo {
    public static void revDelay(String queueName) throws Exception {
       System.out.println("消费者revDelay[{}]开始启动"+queueName);
        final Channel channel;
        try {
            channel = RabbitMQ.rabbit.getChannel();
 
            channel.queueDeclare(queueName, true, false, false, null);
 
            // 死信队列
            HashMap<String, Object> arguments = new HashMap<String, Object>();
            arguments.put("x-dead-letter-exchange", "amq.direct");
            arguments.put("x-dead-letter-routing-key", "message_ttl_routingKey");
 
            channel.queueDeclare(RabbitMQ.delay_qName, true, false, false, arguments);
 
            // 声明死信处理队列
            channel.queueDeclare(queueName, true, false, false, null);
            // 绑定路由
            channel.queueBind(queueName, "amq.direct", "message_ttl_routingKey");
 
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                	long retryCount = 1;
                	Map<String, Object> headers = null;
                	try {
                        headers = properties.getHeaders();
                        if (headers != null) {
                            if (headers.containsKey("x-death")) {
                                List<Map<String, Object>> deaths = (List<Map<String, Object>>) headers.get("x-death");
                                if (deaths.size() > 0) {
                                    Map<String, Object> death = deaths.get(0);
                                    retryCount = (Long) death.get("count");
                                    System.out.println("cishu:"+retryCount);
                                }
                            }
                        }
                    } catch (Exception e) {
                    	e.printStackTrace();
                    }
                	if(headers == null){
                		headers = new HashMap<String, Object>();
                	}
                	
                	
                	String recv = new String(body, "UTF-8");
                	System.out.println(consumerTag+", "+recv+",  "+DateUtil.timeForUnix13());
                	
                	if(retryCount < 4){
                		// requeue：重新入队列，true: 重新放入队列
                		properties = properties.builder().expiration("2000").build();	//延迟2秒
                		SendDemo.getChannel().basicPublish("", RabbitMQ.delay_qName, properties, body);
                	}else{
                		// 确认收到消息并处理完成，删除这条消息
                    	channel.basicAck(envelope.getDeliveryTag(), false);
                	}
                }
            };
            // 打开消息应答机制
            channel.basicConsume(queueName, false, consumer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
 
//        while (true) {
//            try {
//                Thread.sleep(50);
//                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
//                String message = new String(delivery.getBody());
//                if (StringUtils.isBlank(message)) {
//                    continue;
//                }
//                log.info(" [{}] Received [{}]", queueName, message);
// 
//                // 如果没有确认收到，消息队列不会被删除，下个消费者会继续收到此消息，此消费者实例不会收到此消息
//                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
// 
//        }
    }
 
    public static void main(String[] args) throws Exception {
        revDelay(RabbitMQ.qName);
    }
}
