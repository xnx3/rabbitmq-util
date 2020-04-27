package com.xnx3.test;

import com.xnx3.rabbitmq.DelayUtil;

public class SendDemo {
	
	public static void main(String[] args) throws Exception {
		DelayUtil delay = new DelayUtil("t2");
		try {
//			for (int i = 0; i < 10; i++) {
//				delay.send("n"+i,3);
//			}
			delay.send("t22",2);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
