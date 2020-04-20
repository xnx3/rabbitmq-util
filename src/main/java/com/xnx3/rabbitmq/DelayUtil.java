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
import com.xnx3.rabbitmq.interfaces.DelayReceiveInterface;

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
	
	/**
	 * 延时执行。
	 * queueNormalName 、 queueDelayName 是默认的 wangmarket_delay_mq_normal_queue 、 wangmarket_delay_mq_normal_queue_delay
	 */
	public DelayUtil() {
		this.queueNormalName = "wangmarket_delay_mq_normal_queue";
		this.queueDelayName = queueNormalName + "_delay";
		this.failureRetryDelaySecends = new int[]{};
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
	
	/**
	 * 设置消息消费失败后，延迟多长时间后再消费。延迟的时间以此延迟多少次，便是通过这里设置的。
	 * <br/>默认如果不设置，消息只会消费一次，无论消息是消费成功还是失败，都只是消费一次。设置这里之后，如果消费失败，会延迟一段时间后进行重新消费
	 * @param failureRetryDelaySecends 每次延迟的时间，以及延迟多少次。传入数组形式。
	 * 		<br/>例如,传入 new int[]{5,30,300}  则是如果消息在 {@link #receive(DelayReceiveInterface)} 中消费失败，处理失败，那么延迟5秒钟后，会进行第二次消费此消息，如果还是失败，那么再延迟30秒后，第三次消费消息。 如果第三次还是消费失败，那么延迟300秒后，第四次消费消息。
	 */
	public void setFailureRetryDelaySecends(int[] failureRetryDelaySecends) {
		this.failureRetryDelaySecends = failureRetryDelaySecends;
	}
	
	/**
	 * 发送延迟消息
	 * @param msg 要发送的消息字符串
	 * @param delayTime 延迟时间，单位是秒
	 */
	public void send(String msg, int delayTime) throws Exception{
		// 死信队列
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("x-dead-letter-exchange", "amq.direct");
		args.put("x-dead-letter-routing-key", "message_ttl_routingKey");
		
		getChannel().queueDeclare(queueDelayName, true, false, false, args);
 
		// 声明死信处理队列
		getChannel().queueDeclare(queueNormalName, true, false, false, null);
		// 当声明队列，不加任何参数，产生的将是一个临时队列，getQueue返回的是队列名称
//        String temporaryQueueName = getChannel().queueDeclare().getQueue();
//        getChannel().queueDeclare(temporaryQueueName, true, false, false, null);
		
		// 绑定路由
		getChannel().queueBind(queueNormalName, "amq.direct", "message_ttl_routingKey");
//		getChannel().queueBind(temporaryQueueName, "amq.direct", "message_ttl_routingKey");
		AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
		// 延时2秒，测试-1秒
		System.out.println((delayTime*1000)+"");
		AMQP.BasicProperties properties = builder.expiration((delayTime*1000)+"").deliveryMode(2).build();
		getChannel().basicPublish("", queueDelayName, properties, msg.getBytes());
		System.out.println("msg推送完毕:"+msg);
	}
	
	/**
	 * 接收到消息后进行处理
	 * @param receiveInterface 接收到消息后进行处理的接口，消息消费的实现
	 * @throws IOException
	 */
	public void receive(final DelayReceiveInterface receiveInterface) throws IOException{
		getChannel().queueDeclare(queueNormalName, true, false, false, null);

        // 死信队列
        HashMap<String, Object> arguments = new HashMap<String, Object>();
        arguments.put("x-dead-letter-exchange", "amq.direct");
        arguments.put("x-dead-letter-routing-key", "message_ttl_routingKey");

//        getChannel().queueDeclare(RabbitMQ.delay_qName, true, false, false, arguments);
        getChannel().queueDeclare(queueDelayName, true, false, false, arguments);
        
        // 声明死信处理队列
        getChannel().queueDeclare(queueNormalName, true, false, false, null);
        // 绑定路由
        getChannel().queueBind(queueNormalName, "amq.direct", "message_ttl_routingKey");

        Consumer consumer = new DefaultConsumer(getChannel()) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            	long retryCount = 1;
            	Map<String, Object> headers = null;
            	if(failureRetryDelaySecends.length > 0){
                	try {
                        headers = properties.getHeaders();
                        if (headers != null) {
                        	System.out.println(headers.get("x-death"));
                            if (headers.containsKey("x-death")) {
                                List<Map<String, Object>> deaths = (List<Map<String, Object>>) headers.get("x-death");
                                if (deaths.size() > 0) {
                                    Map<String, Object> death = deaths.get(0);
                                    retryCount = (Long) death.get("count");
//                                    System.out.println("cishu:"+retryCount);
//                                    death.put("count", retryCount+1);
                                }
                            }
                        }
                    } catch (Exception e) {
                    	e.printStackTrace();
                    }
            	}
            	
            	String msg = new String(body, "UTF-8");
//            	if(msg == null){
//            		//消息内容为null，异常,那么将当前的重试次数设置为 99999，不需要重试了，直接删除掉
//					retryCount = 99999;
//            	}
            	System.out.println(retryCount+","+(failureRetryDelaySecends.length)+" --- "+envelope.getDeliveryTag());
            	if(retryCount <= failureRetryDelaySecends.length+1){
            		// requeue：重新入队列，true: 重新放入队列
            		
        			if(receiveInterface.dispose(msg)){
        				//返回true，那么处理正常，删除这条消息
        				getChannel().basicAck(envelope.getDeliveryTag(), false);
        			}else{
        				//返回false，那么处理失败，将这条消息重新投递到死信队列
        				
        				//判断是否还可以有下次重试
        				if(failureRetryDelaySecends.length == 0){
        					//没有设置失败重试，那么无需再重复投递
        				}else{
        					//设置了消费失败重试，重复投递消息
        					
        					//retryCount 为重复投递的次数，例如第一次消费，retryCount为1，失败，重新投递一次，那么这里接收到还是1，又失败，在投递一次，这里就变成了2
        					if(retryCount <= failureRetryDelaySecends.length){
            					//还可以有下一次，将消息在发出去
            					int delayTime = failureRetryDelaySecends[(int)(retryCount-1)]*1000;
                				System.out.println("retryCount:"+retryCount+","+"delayTime:"+delayTime);
                				properties = properties.builder().expiration(delayTime+"").build();	//延迟秒数设定
//                        		getChannel().basicPublish("", RabbitMQ.delay_qName, properties, body);
                        		getChannel().basicPublish("", queueDelayName, properties, body);
//                        		getChannel().basicNack(envelope.getDeliveryTag(), false, false);
            				}else{
            					//不再有下一次了，直接删除掉，消费掉
            					System.out.println("去掉这条消息:"+msg);
            					receiveInterface.failure(msg);
                        		// 确认收到消息并处理完成，删除这条消息
            					getChannel().basicAck(envelope.getDeliveryTag(), false);
            				}
        				}
        			}
            	}else{
            		//超过最大重试此处，那么这条消息丢弃掉，也就是标注为处理完成，删除这条消息。正常来说，这个不应该存在的，多判断一下吧
            		System.out.println("删除这条消息:"+msg);
            		receiveInterface.failure(msg);
            		// 确认收到消息并处理完成，删除这条消息
            		getChannel().basicAck(envelope.getDeliveryTag(), false);
            	}
            }
        };
        // 打开消息应答机制
        getChannel().basicConsume(queueNormalName, true, consumer);
        System.out.println("创建接收监听");
	}
	
}
