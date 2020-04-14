package com.xnx3.rabbitmq;


public class RabbitMQ {
	public static RabbitUtil rabbit = new RabbitUtil("114.116.216.206", "admin", "2iNSg24bhjb", 5672);
	public static final String qName = "eshop_update_activity_product_price";
	public static final String delay_qName = qName + "_delay";;
	
}
