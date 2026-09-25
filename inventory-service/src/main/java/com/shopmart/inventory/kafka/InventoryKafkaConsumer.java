package com.shopmart.inventory.kafka;

import com.shopmart.inventory.event.KafkaTopics;
import com.shopmart.inventory.event.OrderEvent;
import com.shopmart.inventory.event.SagaEventType;
import com.shopmart.inventory.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryKafkaConsumer {

    private final ProductService productService;
    private final InventoryKafkaProducer kafkaProducer;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "inventory-service-group")
    public void consume(OrderEvent event) {
        log.info("Inventory-service consumed event type={} for orderId={}", event.getType(), event.getOrderId());

        if (event.getType() == SagaEventType.ORDER_CREATED) {
            handleOrderCreated(event);
        } else if (event.getType() == SagaEventType.PAYMENT_FAILED) {
            handlePaymentFailed(event);
        }
    }

    private void handleOrderCreated(OrderEvent event) {
        try {
            productService.decreaseStock(event.getProductId(), event.getQuantity());
            log.info("Inventory deducted for orderId={}, productId={}, quantity={}",
                    event.getOrderId(), event.getProductId(), event.getQuantity());
            OrderEvent reserved = OrderEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(event.getProductId())
                    .quantity(event.getQuantity())
                    .amount(event.getAmount())
                    .type(SagaEventType.INVENTORY_RESERVED)
                    .message("Inventory reserved successfully")
                    .build();
            kafkaProducer.publishOrderEvent(reserved);
        } catch (Exception e) {
            log.error("Inventory deduction failed for orderId={}: {}", event.getOrderId(), e.getMessage());
            OrderEvent failed = OrderEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(event.getProductId())
                    .quantity(event.getQuantity())
                    .amount(event.getAmount())
                    .type(SagaEventType.INVENTORY_FAILED)
                    .message("Inventory failed: " + e.getMessage())
                    .build();
            kafkaProducer.publishOrderEvent(failed);
        }
    }

    private void handlePaymentFailed(OrderEvent event) {
        try {
            productService.increaseStock(event.getProductId(), event.getQuantity());
            log.info("Inventory restored (compensation) for orderId={}, productId={}, quantity={}",
                    event.getOrderId(), event.getProductId(), event.getQuantity());
            OrderEvent released = OrderEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(event.getProductId())
                    .quantity(event.getQuantity())
                    .amount(event.getAmount())
                    .type(SagaEventType.INVENTORY_RELEASED)
                    .message("Inventory restored due to payment failure")
                    .build();
            kafkaProducer.publishOrderEvent(released);
        } catch (Exception e) {
            log.error("Inventory restore failed for orderId={}: {}", event.getOrderId(), e.getMessage());
        }
    }
}
