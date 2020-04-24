package com.xnx3.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.xnx3.DateUtil;
import com.xnx3.rabbitmq.DelayUtil;
import com.xnx3.rabbitmq.interfaces.DelayReceiveInterface;

public class RevDemo {
    public static void main(String[] args) throws Exception {
        
    	final DelayUtil delay = new DelayUtil("mmm1");
		delay.setFailureRetryDelaySecends(new int[]{1});
		
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
//			delay.send("lll.1", 1);
//			delay.send("lll.2", 1);
			delay.send("lll.3", 3);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
		
//		new Thread(new Runnable() {
//			public void run() {
//				int i = 1;
//				while(true){
//					i++;
//					try {
//						delay.send("lll."+i, 1);
//					} catch (Exception e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//					try {
//						Thread.sleep(1000);
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}
//			}
//		}).start();
    }
}
