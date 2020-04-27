package com.xnx3.demo.delay.full;

import java.io.IOException;
import com.xnx3.rabbitmq.DelayUtil;
import com.xnx3.rabbitmq.RabbitUtil;

/**
 * 发送延迟消息。
 * 分布式部署情况下，发送方肯定会在多个服务器或容器中，每个应用都是一个发送方
 * @author 管雷鸣
 *
 */
public class SendDemo {
	public static void main(String[] args) throws IOException {
		/*
		 * 创建，初始化 DelayUtil 对象
		 * 第一个参数，起一个名字，分布式架构中，无论是多少生产者，还是多少消费者，都用这同一个名字。也就是一个项目中的都用同一个名字
		 * 传入参数为rabbitmq的ip、username、password、端口号
		 */
		DelayUtil delayUtil = new DelayUtil("wangmarket_shop",new RabbitUtil("114.116.216.206", "admin", "2iNSg24bhjb", 5672));
		//发送两条消息
		delayUtil.send("哈哈，我延迟3秒", 3);
		delayUtil.send("哈哈，我不一样，我延迟5秒", 5);
	}
}
