package com.xnx3.rabbitmq.interfaces;


/**
 * 延迟队列，接收消息的接口。用此来接收延迟的消息进行处理
 * @author 管雷鸣
 *
 */
public interface DelayReceiveInterface {
	
	/**
	 * 接收到的延迟消息进行处理
	 * @param msg 接收到的延迟消息的字符串，这个是任何情况下都不会为 null 的。
	 * @param receivecount 这条消息接收的次数。也就是当前是第几次接收了。比如 1是第一次接收，2是第二次接收
	 * @return 处理结果。
	 * 			<ul>
	 * 				<li>true:这条消息处理成功，将这条消息删除掉，已经正常消费了</li>
	 * 				<li>false:这条消息处理失败，没有正常消费，那么这条消息不会删除，会延长一段时间后再次处罚此方法</li>
	 * 			</ul>
	 */
	public boolean dispose(String msg, int receivecount);
	
	/**
	 * 消费失败后执行的
	 * @param msg 接收到的消息的字符串，也就是没有成功消费的这个消息。这个是任何情况下都不会为 null 的。
	 */
	public void failure(String msg);
}
