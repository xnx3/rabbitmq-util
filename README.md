# rabbitmq-util
mq快速使用

## maven 使用
引入rabbitmq client的jar包
````
<dependency>
	<groupId>com.rabbitmq</groupId>
	<artifactId>amqp-client</artifactId>
	<version>5.8.0</version>
</dependency>
````
引入本项目 rabbitmq-1.0.jar，将这个jar下载下来，放到项目的 /src/main/webapp/WEB-INF/lib/ 文件夹下
````
<dependency> 
	<groupId>com.xnx3.rabbitmq</groupId>  
	<artifactId>rabbitmq</artifactId>  
	<version>1.0</version>  
	<scope>system</scope>  
	<systemPath>${project.basedir}/src/main/webapp/WEB-INF/lib/rabbitmq-1.0.jar</systemPath>
</dependency>
````

## 代码书写
#### 单机版演示，适用于单台服务器运行的，入门看这个
````
package com.xnx3.demo.delay.simple;

import java.io.IOException;
import java.util.Date;
import com.xnx3.rabbitmq.DelayUtil;
import com.xnx3.rabbitmq.interfaces.DelayReceiveInterface;

/**
 * 消费者，也就是消息接受者。这里是最简单的使用方式。
 * 这里将消息生产者（发送）、消费者（接收）一块进行的演示。发送、接收 都在这个类中
 * @author 管雷鸣 www.guanleiming.com
 */
public class Demo {
	static DelayUtil delayUtil;
	static{
		/*
		 * delayUtil 的初始化、以及 调用 delayUtil.receive(...) 接收消息处理消息，只需一次即可。
		 */
		try {
			delayUtil = new DelayUtil();
			delayUtil.receive(new DelayReceiveInterface() {
				public boolean dispose(String msg) {
					//接收到消息，消息在这里进行处理。
					System.out.println("消息内容为:"+msg+",time:"+new Date().getTime());
					/*
					 * 如果处理成功，则返回true，证明已经消费成功了。
					 * 如果没处理成功，返回false，那么证明没消费成功，会触发下面的 failure(msg) 接口 
					 */
					return false;
				}
				public void failure(String msg) {
					System.out.println("当消息消费失败后，执行此处。这里更多的多用是，当消息执行失败后，通过这里记录日志，或者发邮件、短信通知开发者。这里接收到的信息为 :"+msg);
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
````

#### 分布式部署方式，多服务器版本

消息接收，消费者。这个部署于某台单独服务器上，用于接收消息进行处理
````
package com.xnx3.demo.delay.full;

import java.io.IOException;
import java.util.Date;
import com.xnx3.rabbitmq.DelayUtil;
import com.xnx3.rabbitmq.RabbitUtil;
import com.xnx3.rabbitmq.interfaces.DelayReceiveInterface;

/**
 * 消费者，也就是消息接受者
 * 在分布式架构中，生产者是有多个，而消费者可能是部署在一台单独的服务器上，进行处理指定的事情
 * @author 管雷鸣
 */
public class ReceiveDemo {
	public static void main(String[] args) throws IOException {
		/*
		 * 创建，初始化 DelayUtil 对象
		 * 第一个参数，起一个名字，分布式架构中，无论是多少生产者，还是多少消费者，都用这同一个名字。也就是一个项目中的都用同一个名字
		 * 传入参数为rabbitmq的ip、username、password、端口号
		 */
		DelayUtil delayUtil = new DelayUtil("wangmarket_shop", new RabbitUtil("114.116.216.206", "admin", "2iNSg24bhjb", 5672));
		/*
		 * 设置消费者接收消息进行处理时，如果处理失败，进行延迟重试的规则。
		 * int数组，但是是秒。比如这里是2、5、10三个，那么会延迟重试三次。数组内有几个数，便是重试几次。这个数是重试的时间间隔
		 * 比如这里的便是 处理失败，那么延迟2秒钟后，会进行第二次消费此消息，如果还是失败，那么再延迟5秒后，第三次消费消息。 如果第三次还是消费失败，那么延迟10秒后，第四次消费消息。如果第四次消费还是失败，那么会触发 DelayReceiveInterface.failure 接口 
		 */
		delayUtil.setFailureRetryDelaySecends(new int[]{2,5,10});
		//实现延迟消息处理接口 DelayReceiveInterface，来接收消息进行处理
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
	}
}
````

消息发送，生产者。分布式部署，那么每个服务器中的都会有这个生产者产生消息。
````
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

````

