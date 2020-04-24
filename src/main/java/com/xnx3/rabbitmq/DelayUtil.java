package com.xnx3.rabbitmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
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
	public String name;	//正常的，接收消息延迟到期后过来的消息，通知的队列，用来消费消息
	public int[] failureRetryDelaySecends;	//执行失败后重试，所延迟等待的秒数，延迟多长时间后重试。数组形式存在，单位是秒。如 [1,5,10] 则是执行失败后，1秒钟后重试一次，若还是失败，则5秒钟后再执行一次，如果还是失败，则10秒钟后再执行一次
	private String routingKey = "message_ttl_routingKey";
	
	//用来进行延迟消息的队列跟交换机，发送的消息先进入这里进行延迟，延迟结束之后，在投递到 consumeExchangeName
	private String delayQueueName;
	private String delayExchangeName;
	
	//用来接收延迟结束之后投递过来的消息的队列跟交换机。接收到延迟结束的消息，用来进行消费使用
	private String consumeQueueName;
	private String consumeExchangeName;
	
	/**
	 * 延时执行。
	 * @param queueNormalName 正常的，接收消息通知的队列的名字，如 wangmarket
	 * @param queueDelayName 死信队列的名字，用来做延迟的队列，延迟完后会将消息加入到 queueNormalName 再被消费。如 wangmarket_delay
	 * @throws IOException 
	 */
	public DelayUtil(String name) throws IOException {
		this.name = name;
		this.failureRetryDelaySecends = new int[]{};
		setParam();
		//声明来接收消息的交换机跟队列
//		declareConsumeQueueAndExchange();
//		//声明用来延迟消息的交换机、队列
		declareDelayQueueAndExchange();
	}
	
	/**
	 * 延时执行。
	 * queueNormalName 、 queueDelayName 是默认的 wangmarket_delay_mq_normal_queue 、 wangmarket_delay_mq_normal_queue_delay
	 * @throws IOException 
	 */
	public DelayUtil() throws IOException {
		this.name = "wangmarket__delaymq_default_queue";
		this.failureRetryDelaySecends = new int[]{};
		setParam();
		//声明来接收消息的交换机跟队列
//		declareConsumeQueueAndExchange();
		//声明用来延迟消息的交换机、队列
		declareDelayQueueAndExchange();
	}
	
	/**
	 * 初始化设置参数
	 */
	private void setParam(){
		this.delayQueueName = "delay_"+this.name+"_queue";
		this.delayExchangeName = "delay_"+this.name+"_exchange";
		this.consumeQueueName = "consume_"+this.name+"_queue";
		this.consumeExchangeName = "consume_"+this.name+"_exchange";
//		this.routingKey = "routingkey_"+this.name;
	}
	
	/**
	 * 创建用来接收延迟的消息用来消费的队列跟交换机，并绑定
	 * @throws IOException
	 */
	private void declareConsumeQueueAndExchange() throws IOException{
//		this.consumeExchangeName = "amq.direct";
		//队列
		getChannel().queueDeclare(this.consumeQueueName, true, false, false, null);	
		//交换机
		getChannel().exchangeDeclare(this.consumeExchangeName, BuiltinExchangeType.DIRECT);
		// 绑定路由
        getChannel().queueBind(this.consumeQueueName, this.consumeExchangeName, routingKey);
	}
	
	/**
	 * 创建用来延迟的队列跟交换机，并绑定
	 * @throws IOException
	 */
	private void declareDelayQueueAndExchange() throws IOException{
//		String id = UUID.randomUUID()+"";
//		this.delayQueueName = id+"_queue";
//        this.delayExchangeName = id + "_exchange";
//        this.delayExchangeName = "amq.direct";
		
		// 用来延迟的队列
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("x-dead-letter-exchange", this.consumeExchangeName);
		args.put("x-dead-letter-routing-key", routingKey);
		getChannel().queueDeclare(this.delayQueueName, true, false, false, args);	//创建队列
		getChannel().exchangeDeclare(this.delayExchangeName, BuiltinExchangeType.DIRECT);	//创建延迟队列的交换机
		// 绑定路由
		getChannel().queueBind(this.delayQueueName, this.delayExchangeName, routingKey);

		System.out.println("创建临时队列："+this.delayQueueName);
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
		AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
		// 延时2秒，测试-1秒
		AMQP.BasicProperties properties = builder.expiration((delayTime*1000)+"").deliveryMode(2).build();
		getChannel().basicPublish(this.delayExchangeName, this.routingKey, properties, msg.getBytes());
		System.out.println("msg推送完毕:"+msg);
	}
	
	/**
	 * 接收到消息后进行处理
	 * @param receiveInterface 接收到消息后进行处理的接口，消息消费的实现
	 * @throws IOException
	 */
	public void receive(final DelayReceiveInterface receiveInterface) throws IOException{
		//声明用来接收延迟消息的交换机、队列
		declareConsumeQueueAndExchange();
				
		Consumer consumer = new DefaultConsumer(getChannel()) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            	try {

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
                                    }
                                }
                            }
                        } catch (Exception e) {
                        	e.printStackTrace();
                        }
                	}
                	
                	String msg = new String(body, "UTF-8");
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
            					//没有设置失败重试，那么无需再重复投递,直接调用处理失败接口
            					receiveInterface.failure(msg);
            				}else{
            					//设置了消费失败重试，重复投递消息
            					
            					//retryCount 为重复投递的次数，例如第一次消费，retryCount为1，失败，重新投递一次，那么这里接收到还是1，又失败，在投递一次，这里就变成了2
            					if(retryCount <= failureRetryDelaySecends.length){
                					//还可以有下一次，将消息在发出去
                					int delayTime = failureRetryDelaySecends[(int)(retryCount-1)]*1000;
                    				System.out.println("retryCount:"+retryCount+","+"delayTime:"+delayTime);
                    				properties = properties.builder().expiration(delayTime+"").build();	//延迟秒数设定
                    				try {
    									failureRetry(properties, body);
    								} catch (Exception e) {
    									e.printStackTrace();
    								}
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
                
				} catch (Exception e) {
					e.printStackTrace();
				}
            }
        };
        // 打开消息应答机制
        getChannel().basicConsume(this.consumeQueueName, true, consumer);
        System.out.println("创建接收监听");
	}
	
	
//	private String failureRetryTemporaryQueueName = null;
	/**
	 * 消息消费失败，重试，重新投递到延迟队列中
	 * @param msg 要发送的消息字符串
	 * @param delayTime 延迟时间，单位是秒
	 */
	public void failureRetry(BasicProperties properties, byte[] body) throws Exception{
        Map<String, Object> headers = null;
        String delayQueue = null;	//死信队列，用来做延迟的，将消息重新投递到这里
        System.out.println("---");
        try {
            headers = properties.getHeaders();
            if (headers != null) {
                if (headers.containsKey("x-death")) {
                    List<Map<String, Object>> deaths = (List<Map<String, Object>>) headers.get("x-death");
                    if (deaths.size() > 0) {
                    	Map<String, Object> death = deaths.get(0);
                    	delayQueue = death.get("queue").toString();
                    	System.out.println("原本的queue："+delayQueue);
                    }
                }
            }
        } catch (Exception e) {
        	e.printStackTrace();
        }
        
//		getChannel().queueDeclare(queueDelayName, true, false, false, args);
		// 当声明队列，不加任何参数，产生的将是一个临时队列，getQueue返回的是队列名称
//        String temporaryQueueName = getChannel().queueDeclare().getQueue();
        //getChannel().queueDeclare(temporaryQueueName, true, false, false, null);
//		getChannel().basicPublish("", this.routingKey, properties, body);
		getChannel().basicPublish(this.delayExchangeName, this.routingKey, properties, body);
	}
}
