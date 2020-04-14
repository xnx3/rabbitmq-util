package com.xnx3.rabbitmq;

import java.io.IOException;

public class Test {
	
	public static void main(String[] args) {
		DelayUtil delay = new DelayUtil("w","w_delay");
		try {
			delay.send("hahah888");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			delay.receive();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
