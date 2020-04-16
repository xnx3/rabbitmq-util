package com.xnx3.rabbitmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.xnx3.DateUtil;
import com.xnx3.rabbitmq.interfaces.DelayReceiveInterface;

import net.sf.json.JSONObject;

/**
 * 延时执行。
 * 比如指定一个消息，延迟10秒后才会被接收
 * @author 管雷鸣
 *
 */
public class DelayUtil {
	public String queueNormalName;	//正常的，接收消息通知的队列
	public String queueDelayName;	//死信队列，用来做延迟的队列，延迟完后会将消息加入到 queueNormalName 再被消费
	public int[] failureRetryDelaySecends;	//执行失败后重试，所延迟等待的秒数，延迟多长时间后重试。数组形式存在，单位是秒。如 [1,5,10] 则是执行失败后，1秒钟后重试一次，若还是失败，则5秒钟后再执行一次，如果还是失败，则10秒钟后再执行一次
	public DelayReceiveInterface receiveInterface;	//接收到消息后进行处理
	
	
	/**
	 * 延时执行。
	 * @param queueNormalName 正常的，接收消息通知的队列的名字，如 wangmarket
	 * @param queueDelayName 死信队列的名字，用来做延迟的队列，延迟完后会将消息加入到 queueNormalName 再被消费。如 wangmarket_delay
	 */
	public DelayUtil(String queueNormalName, String queueDelayName) {
		this.queueNormalName = queueNormalName;
		this.queueDelayName = queueDelayName;
		this.failureRetryDelaySecends = new int[]{};
	}
	
	public void setReceiveInterface(DelayReceiveInterface receiveInterface) {
		this.receiveInterface = receiveInterface;
	}



	private Channel channel;
	/**
	 * 获取一个通道
	 */
	public Channel getChannel(){
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
	
	public void send(String msg) throws Exception{
		// 死信队列
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("x-dead-letter-exchange", "amq.direct");
		args.put("x-dead-letter-routing-key", "message_ttl_routingKey");
		getChannel().queueDeclare(queueDelayName, true, false, false, args);
 
		// 声明死信处理队列
		getChannel().queueDeclare(queueNormalName, true, false, false, null);
 
		// 绑定路由
		getChannel().queueBind(queueNormalName, "amq.direct", "message_ttl_routingKey");
		AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
		// 延时2秒，测试-1秒
		AMQP.BasicProperties properties = builder.expiration("10000").deliveryMode(2).build();
		getChannel().basicPublish("", queueDelayName, properties, msg.getBytes());
//		getChannel().close();
		System.out.println("msg=[{}]推送完毕"+msg);
	}
	
	public void receive() throws IOException{
		getChannel().queueDeclare(queueNormalName, true, false, false, null);

        // 死信队列
        HashMap<String, Object> arguments = new HashMap<String, Object>();
        arguments.put("x-dead-letter-exchange", "amq.direct");
        arguments.put("x-dead-letter-routing-key", "message_ttl_routingKey");

        getChannel().queueDeclare(RabbitMQ.delay_qName, true, false, false, arguments);

        // 声明死信处理队列
        getChannel().queueDeclare(queueNormalName, true, false, false, null);
        // 绑定路由
        getChannel().queueBind(queueNormalName, "amq.direct", "message_ttl_routingKey");

        Consumer consumer = new DefaultConsumer(getChannel()) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            	long retryCount = 1;
            	if(failureRetryDelaySecends.length > 0){
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
//                	if(headers == null){
//                		headers = new HashMap<String, Object>();
//                	}
            	}
            	
            	String recv = new String(body, "UTF-8");
            	JSONObject json = null;
            	try {
            		json = JSONObject.fromObject(recv);
				} catch (Exception e) {
					//json解析异常,那么将当前的重试次数设置为 99999，不需要重试了，直接删除掉
					retryCount = 99999;
				}
            	System.out.println(consumerTag+", "+recv+",  "+DateUtil.timeForUnix13());
            	int allowRetryNumber = failureRetryDelaySecends.length+1;	//如果执行出错，允许重试的次数。例如如果为2，则是允许最多执行2次 
            	if(retryCount <= failureRetryDelaySecends.length){
            		// requeue：重新入队列，true: 重新放入队列
            		if(receiveInterface != null){
            			if(receiveInterface.dispose(json)){
            				//返回true，那么处理正常，删除这条消息
            				channel.basicAck(envelope.getDeliveryTag(), false);
            			}else{
            				//返回false，那么处理失败，将这条消息重新投递到死信队列
            				properties = properties.builder().expiration("2000").build();	//延迟2秒
                    		getChannel().basicPublish("", RabbitMQ.delay_qName, properties, body);
            			}
            		}
            	}else{
            		//超过最大重试此处，那么这条消息丢弃掉，也就是标注为处理完成，删除这条消息
            		if(json == null){
            			//json解析异常，没有解析出数据来
            			System.out.println("json解析异常，没有解析出数据来:"+recv);
            		}
            		
            		// 确认收到消息并处理完成，删除这条消息
                	channel.basicAck(envelope.getDeliveryTag(), false);
            	}
            }
        };
        // 打开消息应答机制
        getChannel().basicConsume(queueNormalName, false, consumer);
	}
	
}
