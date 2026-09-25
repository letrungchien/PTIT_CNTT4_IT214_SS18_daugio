package com.shopmart.order.kafka;

import com.shopmart.order.event.KafkaTopics;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.SagaEventType;
import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderKafkaConsumer {

    private final OrderService orderService;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "order-service-group")
    public void consume(OrderEvent event) {
        log.info("Order-service consumed event type={} for orderId={}", event.getType(), event.getOrderId());
        if (event.getType() == SagaEventType.PAYMENT_COMPLETED) {
            log.info("Payment success -> completing order id={}", event.getOrderId());
            orderService.completeOrder(event.getOrderId());
        } else if (event.getType() == SagaEventType.INVENTORY_RELEASED) {
            log.info("Inventory released (compensation done) -> cancelling order id={}", event.getOrderId());
            orderService.cancelOrder(event.getOrderId(), event.getMessage());
        } else if (event.getType() == SagaEventType.INVENTORY_FAILED) {
            log.info("Inventory failed -> cancelling order id={}", event.getOrderId());
            orderService.cancelOrder(event.getOrderId(), event.getMessage());
        }
    }
}
