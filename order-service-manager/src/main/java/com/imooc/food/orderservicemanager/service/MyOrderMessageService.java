package com.imooc.food.orderservicemanager.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.imooc.food.orderservicemanager.dao.OrderDetailDao;
import com.imooc.food.orderservicemanager.dto.OrderMessageDTO;
import com.imooc.food.orderservicemanager.enummeration.OrderStatus;
import com.imooc.food.orderservicemanager.po.OrderDetailPO;
import com.rabbitmq.client.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @description: 消息处理相关业务逻辑
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-08-22 20:13
 **/
@Slf4j
public class MyOrderMessageService {

    @Autowired
    private OrderDetailDao orderDetailDao;

    ObjectMapper objectMapper = new ObjectMapper();

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

    DeliverCallback deliverCallback = (consumerTag, message) -> {
        String messageBody = new String(message.getBody());
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("myhost");

        try {
            OrderMessageDTO orderMessageDTO = objectMapper.readValue(messageBody, OrderMessageDTO.class);

            OrderDetailPO orderDetailPO = orderDetailDao.selectOrder(orderMessageDTO.getOrderId());
            switch (orderDetailPO.getStatus()) {

                case ORDER_CREATING:
                    //订单被确认，并且价格被商家赋值了
                    if (orderMessageDTO.getConfirmed() && null != orderMessageDTO.getPrice()) {
                        orderDetailPO.setStatus(OrderStatus.RESTAURANT_CONFIRMED);
                        orderDetailPO.setPrice(orderDetailPO.getPrice());
                        orderDetailDao.update(orderDetailPO);
                        //发送商家处理好的订单消息给骑手微服务
                        try (Connection connection = connectionFactory.newConnection();
                             Channel channel = connection.createChannel()) {
                            String messageToSend = objectMapper.writeValueAsString(orderMessageDTO);
                            channel.basicPublish("exchange.order.deliveryman",
                                    "key.deliveryman", null, messageToSend.getBytes());
                        }
                    } else {
                        //订单失败
                        orderDetailPO.setStatus(OrderStatus.FAILED);
                        orderDetailDao.update(orderDetailPO);
                    }
                    break;
                case RESTAURANT_CONFIRMED:
                    break;
                case DELIVERYMAN_CONFIRMED:
                    break;
                case SETTLEMENT_CONFIRMED:
                    break;
                case ORDER_CREATED:
                    break;
                case FAILED:
                    break;
            }
        } catch (Exception e) {
            log.error("",e);
        }
    };


}
