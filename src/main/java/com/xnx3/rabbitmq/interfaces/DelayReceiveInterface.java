package com.xnx3.rabbitmq.interfaces;

import net.sf.json.JSONObject;

/**
 * 延迟队列，接收消息的接口。用此来接收延迟的消息进行处理
 * @author 管雷鸣
 *
 */
public interface DelayReceiveInterface {
	
	/**
	 * 接收到的延迟消息进行处理
	 * @param json 接收到的延迟消息的json，这个是任何情况下都不会为 null 的
	 * @return 处理结果。
	 * 			<ul>
	 * 				<li>true:这条消息处理成功，将这条消息删除掉，已经正常消费了</li>
	 * 				<li>false:这条消息处理失败，没有正常消费，那么这条消息不会删除，会延长一段时间后再次处罚此方法</li>
	 * 			</ul>
	 */
	public boolean dispose(JSONObject json);
	
}
