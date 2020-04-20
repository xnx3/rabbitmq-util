package com.xnx3.rabbitmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.xnx3.DateUtil;

public class SendDemo {
	public static Channel channel;
	public static Channel getChannel(){
		if(channel == null){
			try {
				channel = RabbitMQ.rabbit.getConnection().createChannel();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				e.printStackTrace();
			}
		}
		return channel;
	}
	
	public static void sendDelay(String msg) throws Exception {
		// 死信队列
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("x-dead-letter-exchange", "amq.direct");
		args.put("x-dead-letter-routing-key", "message_ttl_routingKey");
		getChannel().queueDeclare(RabbitMQ.delay_qName, true, false, false, args);
 
		// 声明死信处理队列
		getChannel().queueDeclare(RabbitMQ.qName, true, false, false, null);
 
		// 绑定路由
		getChannel().queueBind(RabbitMQ.qName, "amq.direct", "message_ttl_routingKey");
		AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
		// 延时2秒，测试-1秒
		AMQP.BasicProperties properties = builder.expiration("10000").deliveryMode(2).build();
		getChannel().basicPublish("", RabbitMQ.delay_qName, properties, msg.getBytes());
		getChannel().close();
		System.out.println("msg=[{}]推送完毕"+msg);
	}
 
	public static void main(String[] args) throws Exception {
		String msg= "haha1_" + DateUtil.timeForUnix13();
		sendDelay(msg);
	}
}
