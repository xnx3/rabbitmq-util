package com.xnx3.test;

import java.io.IOException;

import com.xnx3.rabbitmq.DelayUtil;
import com.xnx3.rabbitmq.interfaces.DelayReceiveInterface;


public class Test {
	
	public static void main(String[] args) {
		DelayUtil delay = new DelayUtil("e","e_delay");
		delay.setFailureRetryDelaySecends(new int[]{1,2,3});
		try {
			delay.send("b",3);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
}
