package com.xnx3.rabbitmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.xnx3.rabbitmq.interfaces.DelayReceiveInterface;

/**
 * 延时执行。
 * 比如指定一个消息，延迟10秒后才会被接收
 * 
 * 使用说明：
 * 需要先创建消费者 {@link #receive(DelayReceiveInterface)} ，再创建生产者 {@link #send(String, int)} ,如果先生产，再创建消费者，时间上会出问题，导致消费者接收不到消息
 * @author 管雷鸣
 *
 */
public class DelayUtil {
	public String name;	//正常的，接收消息延迟到期后过来的消息，通知的队列，用来消费消息
	public int[] failureRetryDelaySecends;	//执行失败后重试，所延迟等待的秒数，延迟多长时间后重试。数组形式存在，单位是秒。如 [1,5,10] 则是执行失败后，1秒钟后重试一次，若还是失败，则5秒钟后再执行一次，如果还是失败，则10秒钟后再执行一次
	private String routingKey = "message_ttl_routingKey";
	private RabbitUtil rabbitUtil;	//rabbitUtil 的工具类
	
	//用来进行延迟消息的队列跟交换机，发送的消息先进入这里进行延迟，延迟结束之后，在投递到 consumeExchangeName
	private String delayQueueName;
	private String delayExchangeName;
	
	//用来接收延迟结束之后投递过来的消息的队列跟交换机。接收到延迟结束的消息，用来进行消费使用
	private String consumeQueueName;
	private String consumeExchangeName;
	
	/**
	 * 延时执行。
	 * @param name 延迟队列的标识，名字。同一个名字，即使是 new 了多个 DelayUtil ，也都是同一个消息通道。
	 * @param rabbitUtil rabbit的一些配置信息。传入如： 
	 * <pre>
	 * 	new RabbitUtil("114.116.216.206", "admin", "12345678", 5672);
	 * </pre>
	 * @throws IOException 
	 */
	public DelayUtil(String name, RabbitUtil rabbitUtil) throws IOException {
		this.name = name;
		this.rabbitUtil = rabbitUtil;
		setParam();
		//声明来接收消息的交换机跟队列,这里放在创建接收回调时再创建
//		declareConsumeQueueAndExchange();
		//声明用来延迟消息的交换机、队列
		declareDelayQueueAndExchange();
	}
	
	/**
	 * 延时执行。单项目使用，非分布式的使用
	 * @param rabbitUtil rabbit的一些配置信息。传入如： 
	 * <pre>
	 * 	new RabbitUtil("114.116.216.206", "admin", "12345678", 5672);
	 * </pre>
	 * @throws IOException 
	 */
	public DelayUtil(RabbitUtil rabbitUtil) throws IOException {
		this.rabbitUtil = rabbitUtil;
		this.name = "wangmarket_delaymq_default_queue";
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
//		this.consumeExchangeName = "consume_"+this.name+"_exchange";
		//用存在的 amq.direct 
		this.consumeExchangeName = "amq.direct";
		
		//设置路由
		this.routingKey = name+"_delay_routingKey";
		
		//设置默认的消息重试延迟时间
		this.failureRetryDelaySecends = new int[]{};
	}
	
	/**
	 * 创建用来接收延迟的消息用来消费的队列跟交换机，并绑定
	 * @throws IOException
	 */
	private void declareConsumeQueueAndExchange() throws IOException{
		//队列
		getOpenChannel().queueDeclare(this.consumeQueueName, true, false, false, null);	
		//交换机
//		getOpenChannel().exchangeDeclare(this.consumeExchangeName, BuiltinExchangeType.DIRECT);
		//绑定路由
		getOpenChannel().queueBind(this.consumeQueueName, this.consumeExchangeName, routingKey);
	}
	
	/**
	 * 创建用来延迟的队列跟交换机，并绑定
	 * @throws IOException
	 */
	private void declareDelayQueueAndExchange() throws IOException{
		// 用来延迟的队列
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("x-dead-letter-exchange", this.consumeExchangeName);
		args.put("x-dead-letter-routing-key", routingKey);
		getOpenChannel().queueDeclare(this.delayQueueName, true, false, false, args);	//创建队列
		getOpenChannel().exchangeDeclare(this.delayExchangeName, BuiltinExchangeType.DIRECT);	//创建延迟队列的交换机
		// 绑定路由
		getOpenChannel().queueBind(this.delayQueueName, this.delayExchangeName, routingKey);
		System.out.println("创建临时队列："+this.delayQueueName);
	}
	
	private Channel channel;
	/**
	 * 获取一个打开的通道
	 */
	public Channel getOpenChannel(){
		if(channel == null || !channel.isOpen()){
			try {
				channel = this.rabbitUtil.getConnection().createChannel();
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
	 * 		<br/>例如,传入 <pre> new int[]{5,30,300} </pre> 则是如果消息在 {@link #receive(DelayReceiveInterface)} 中消费失败，处理失败，那么延迟5秒钟后，会进行第二次消费此消息，如果还是失败，那么再延迟30秒后，第三次消费消息。 如果第三次还是消费失败，那么延迟300秒后，第四次消费消息。
	 */
	public void setFailureRetryDelaySecends(int[] failureRetryDelaySecends) {
		this.failureRetryDelaySecends = failureRetryDelaySecends;
	}
	
	
	/**
	 * 发送延迟消息
	 * @param msg 要发送的消息字符串
	 * @param delayTime 延迟时间，单位是秒
	 */
	public void send(String msg, int delayTime) throws IOException{
		send(msg, delayTime,1);
	}
	private void send(String msg, int delayTime, int count) throws IOException{
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("receivecount", count);
		headers.put("delay_exchange_name", this.delayExchangeName);
		AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
		// 延时2秒，测试-1秒
		AMQP.BasicProperties properties = builder.expiration((delayTime*1000)+"").deliveryMode(2).headers(headers).build();
		getOpenChannel().basicPublish(this.delayExchangeName, this.routingKey, properties, msg.getBytes());
	}
	
	/**
	 * 接收到消息后进行处理
	 * @param receiveInterface 接收到消息后进行处理的接口，消息消费的实现
	 * @throws IOException
	 */
	public void receive(final DelayReceiveInterface receiveInterface) throws IOException{
		//声明用来接收延迟消息的交换机、队列
		declareConsumeQueueAndExchange();
		
		Consumer consumer = new DefaultConsumer(getOpenChannel()) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            	try {
                	int receivecount = 1;	//此条消息的接收次数，是第几次接收这个消息。默认为1，第一次接收
                	Map<String, Object> headers = null;
                	if(failureRetryDelaySecends.length > 0){
                    	try {
                            headers = properties.getHeaders();
                            if (headers != null) {
                            	if(headers.get("receivecount") != null){
                            		receivecount = (Integer) headers.get("receivecount");
                            	}
                            }
                        } catch (Exception e) {
                        	e.printStackTrace();
                        }
                	}
                	
                	String msg = new String(body, "UTF-8");
                	if(receivecount <= failureRetryDelaySecends.length+1){
                		// requeue：重新入队列，true: 重新放入队列
                		
                		boolean receiveResult = false;
                		try {
                			receiveResult = receiveInterface.dispose(msg, receivecount);
						} catch (Exception e) {
							e.printStackTrace();
						}
            			if(receiveResult){
            				//返回true，那么处理正常，删除这条消息
            				getOpenChannel().basicAck(envelope.getDeliveryTag(), false);
            			}else{
            				//返回false，那么处理失败，将这条消息重新投递到死信队列
            				
            				//判断是否还可以有下次重试
            				if(receivecount <= failureRetryDelaySecends.length){
            					//还可以有下一次，将消息在发出去
            					int delayTime = failureRetryDelaySecends[(int)(receivecount-1)];
                				properties = properties.builder().expiration(delayTime+"").build();	//延迟秒数设定
                				send(msg, delayTime, receivecount+1);
            				}else{
            					//不再有下一次了，直接删除掉，消费掉
            					try {
            						receiveInterface.failure(msg);
								} catch (Exception e) {
									e.printStackTrace();
								}
                        		// 确认收到消息并处理完成，删除这条消息
            					getOpenChannel().basicAck(envelope.getDeliveryTag(), false);
            				}
            			}
                	}else{
                		//超过最大重试此处，那么这条消息丢弃掉，也就是标注为处理完成，删除这条消息。正常来说，这个不应该存在的，多判断一下吧
                		try {
    						receiveInterface.failure(msg);
						} catch (Exception e) {
							e.printStackTrace();
						}
                		// 确认收到消息并处理完成，删除这条消息
                		getOpenChannel().basicAck(envelope.getDeliveryTag(), false);
                	}
				} catch (Exception e) {
					e.printStackTrace();
				}
            }
        };
        // 打开消息应答，不自动确认接收消息，手动确认接收到消息
        getOpenChannel().basicConsume(this.consumeQueueName, false, consumer);
	}
}
