package com.shopmart.order.kafka;

import com.shopmart.order.event.KafkaTopics;
import com.shopmart.order.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public void publishOrderEvent(OrderEvent event) {
        kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), event);
        log.info("Published event type={} for orderId={}", event.getType(), event.getOrderId());
    }
}
