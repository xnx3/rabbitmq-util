package com.xnx3.demo.delay.simple;

import java.io.IOException;
import java.util.Date;
import com.xnx3.rabbitmq.DelayUtil;
import com.xnx3.rabbitmq.RabbitUtil;
import com.xnx3.rabbitmq.interfaces.DelayReceiveInterface;

/**
 * 延迟消息的demo演示。
 * 这里将消息生产者（发送）、消费者（接收）一块进行的演示。发送、接收 都在这个类中
 * 注意，如果项目不使用分布式，只是单台服务器运行，可以使用此种方式。如果使用分布式，请参考 com.xnx3.demo.delay.full 包的demo
 * @author 管雷鸣
 */
public class Demo {
	static DelayUtil delayUtil;
	static{
		/*
		 * delayUtil 的初始化、以及 调用 delayUtil.receive(...) 接收消息处理消息，只需一次即可。
		 */
		try {
			//创建，初始化 DelayUtil 对象,传入参数为rabbitmq的ip、username、password、端口号
			delayUtil = new DelayUtil(new RabbitUtil("114.116.216.206", "admin", "2iNSg24bhjb", 5672));
			delayUtil.receive(new DelayReceiveInterface() {
				public boolean dispose(String msg, int receivecount) {
					//接收到消息，消息在这里进行处理。
					System.out.println("接收到消息内容为:"+msg+", 当前第"+receivecount+"次接收，time:"+new Date().getTime());
					//这里因为是做演示，故意返回处理失败，让他延迟重试。如果返回true，那么证明这条消息处理完毕，成功。直接将这条消息自动删除掉
					return false;
				}
				public void failure(String msg) {
					//当消息消费失败后，执行此处。这里更多的多用是，当消息执行失败后，通过这里记录日志，或者发邮件、短信通知开发者。
					System.out.println("消费失败的消息 : "+msg);
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException {
		//这里发送延迟消息时，直接通过 delayUtil.send(...) 即可发送延迟消息，延迟多少秒后，自动由 delayUtil.receive(...) 接收
		delayUtil.send("哈哈，我延迟1秒", 1);
		delayUtil.send("哈哈，我延迟2秒", 2);
		delayUtil.send("哈哈，我延迟3秒", 3);
		delayUtil.send("哈哈，我就比较厉害了，我延迟10秒", 10);
	}
}
