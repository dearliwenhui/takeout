package com.imooc.food.orderservicemanager.service;

import com.rabbitmq.client.*;
import lombok.SneakyThrows;

/**
 * @description: 消息处理相关业务逻辑
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-08-22 20:13
 **/
public class MyOrderMessageService {

    /**
     * 声明消息队列、交换机、绑定、消息的处理
     */
    @SneakyThrows
    public void handleMsg() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("myhost");
        try (Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel();) {
            //声明order 和 restaurant 交换消息的交换机
            channel.exchangeDeclare(
                    "exchange.order.restaurant",
                    BuiltinExchangeType.DIRECT,
                    true,
                    false,
                    null
            );
            //如果我们声明一个独占队列(仅限于此连接)，则为True。
            //exclusive设置为true，则除了当前client的connection之外，其他connection都不能使用。
            channel.queueDeclare("queue.order", true, false, false, null);
            channel.queueBind("queue.order", "exchange.order.restaurant", "key.order");

            //声明order和deliveryman 交换消息的交换机
            channel.exchangeDeclare("exchange.order.deliveryman"
                    , BuiltinExchangeType.DIRECT, true, false, null);
            //订单queue已经被申明了，所以可以不需要
//            channel.queueDeclare("queue.order", true, false, false, null);
            channel.queueBind("queue.order", "exchange.order.deliveryman", "key.order");

        }
    }

    public static void main(String[] args) {

    }

}
