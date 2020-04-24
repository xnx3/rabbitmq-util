package com.xnx3.test;

import java.io.IOException;

import com.xnx3.rabbitmq.DelayUtil;
import com.xnx3.rabbitmq.interfaces.DelayReceiveInterface;


public class Test {
	
	public static void main(String[] args) throws IOException {
		DelayUtil delay = new DelayUtil("w");
		delay.setFailureRetryDelaySecends(new int[]{1});
		try {
			delay.send("w",3);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			delay.receive(new DelayReceiveInterface() {
				
				public boolean dispose(String msg) {
					System.out.println("dispose:"+msg);
					return false;
				}

				public void failure(String msg) {
					System.out.println("xiaofei shibai :"+msg);
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
	}
	
}
