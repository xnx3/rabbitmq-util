package com.xnx3.test;

import java.io.IOException;
import com.xnx3.rabbitmq.DelayUtil;
import com.xnx3.rabbitmq.interfaces.DelayReceiveInterface;

public class RevDemo {
    public static void main(String[] args) throws Exception {
        
    	final DelayUtil delay = new DelayUtil("t2");
		delay.setFailureRetryDelaySecends(new int[]{1,5});
		
		try {
			delay.receive(new DelayReceiveInterface() {
				
				public boolean dispose(String msg) {
					System.out.println("dispose:"+msg+" , ");
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
