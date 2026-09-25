package com.shopmart.inventory.kafka;

import com.shopmart.inventory.event.KafkaTopics;
import com.shopmart.inventory.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryKafkaProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public void publishOrderEvent(OrderEvent event) {
        kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), event);
        log.info("Inventory-service published event type={} for orderId={}", event.getType(), event.getOrderId());
    }
}
