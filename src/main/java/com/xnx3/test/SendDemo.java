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
		DelayUtil delay = new DelayUtil("a");
		try {
			delay.send("c74",3);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
