package com.shopmart.payment.kafka;

import com.shopmart.payment.dto.PaymentRequest;
import com.shopmart.payment.dto.PaymentResponse;
import com.shopmart.payment.entity.PaymentStatus;
import com.shopmart.payment.event.KafkaTopics;
import com.shopmart.payment.event.OrderEvent;
import com.shopmart.payment.event.SagaEventType;
import com.shopmart.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentKafkaConsumer {

    private final PaymentService paymentService;
    private final PaymentKafkaProducer kafkaProducer;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "payment-service-group")
    public void consume(OrderEvent event) {
        log.info("Payment-service consumed event type={} for orderId={}", event.getType(), event.getOrderId());

        if (event.getType() == SagaEventType.INVENTORY_RESERVED) {
            handleInventoryReserved(event);
        }
    }

    private void handleInventoryReserved(OrderEvent event) {
        PaymentRequest request = new PaymentRequest(event.getOrderId(), event.getAmount());
        PaymentResponse response = paymentService.processPayment(request);

        if (response.getStatus() == PaymentStatus.SUCCESS) {
            log.info("Payment success for orderId={}", event.getOrderId());
            OrderEvent successEvent = OrderEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(event.getProductId())
                    .quantity(event.getQuantity())
                    .amount(event.getAmount())
                    .type(SagaEventType.PAYMENT_COMPLETED)
                    .message("Payment completed successfully")
                    .build();
            kafkaProducer.publishOrderEvent(successEvent);
        } else {
            log.error("Payment failed for orderId={}: {}", event.getOrderId(), response.getMessage());
            OrderEvent failedEvent = OrderEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(event.getProductId())
                    .quantity(event.getQuantity())
                    .amount(event.getAmount())
                    .type(SagaEventType.PAYMENT_FAILED)
                    .message("Payment failed: " + response.getMessage())
                    .build();
            kafkaProducer.publishOrderEvent(failedEvent);
        }
    }
}
