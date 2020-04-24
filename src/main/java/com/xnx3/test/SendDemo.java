package com.xnx3.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.xnx3.DateUtil;
import com.xnx3.rabbitmq.DelayUtil;

public class SendDemo {
	
	public static void main(String[] args) throws Exception {
		DelayUtil delay = new DelayUtil();
		try {
//			for (int i = 0; i < 10; i++) {
//				delay.send("n"+i,3);
//			}
			delay.send("m1",2);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
