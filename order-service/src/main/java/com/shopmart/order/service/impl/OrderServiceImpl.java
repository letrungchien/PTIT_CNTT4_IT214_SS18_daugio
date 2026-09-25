package com.shopmart.order.service.impl;

import com.shopmart.order.client.InventoryClient;
import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.dto.StockRequest;
import com.shopmart.order.entity.Order;
import com.shopmart.order.entity.OrderStatus;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.SagaEventType;
import com.shopmart.order.exception.ResourceNotFoundException;
import com.shopmart.order.kafka.KafkaProducerService;
import com.shopmart.order.repository.OrderRepository;
import com.shopmart.order.service.OrderService;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final InventoryClient inventoryClient;
    private final KafkaProducerService kafkaProducerService;

    @Override
    @Transactional
    @CircuitBreaker(name = "inventoryService", fallbackMethod = "createOrderFallback")
    public OrderResponse createOrder(OrderRequest request) {
        log.info("Creating order for customerId={}, productId={}, quantity={}",
                request.getCustomerId(), request.getProductId(), request.getQuantity());

        ProductDto product = inventoryClient.getProductById(request.getProductId());
        log.info("Fetched product id={}, price={}, stock={}", product.getId(), product.getPrice(), product.getStock());

        BigDecimal totalAmount = product.getPrice().multiply(BigDecimal.valueOf(request.getQuantity()));

        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .totalAmount(totalAmount)
                .status(OrderStatus.PENDING)
                .build();
        Order saved = orderRepository.save(order);
        log.info("Created order id={} with status PENDING, totalAmount={}", saved.getId(), totalAmount);

        OrderEvent event = OrderEvent.builder()
                .orderId(saved.getId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .amount(totalAmount)
                .type(SagaEventType.ORDER_CREATED)
                .message("Order created, initiating saga")
                .build();
        kafkaProducerService.publishOrderEvent(event);

        return OrderResponse.from(saved);
    }

    public OrderResponse createOrderFallback(OrderRequest request, Throwable t) {
        log.error("Circuit breaker fallback triggered for createOrder: {}", t.getMessage());
        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .status(OrderStatus.CANCELLED)
                .failureReason("Inventory service unavailable: " + t.getMessage())
                .build();
        Order saved = orderRepository.save(order);
        log.error("Order id={} CANCELLED due to inventory service failure", saved.getId());
        return OrderResponse.from(saved);
    }

    @Override
    public OrderResponse getOrderById(Long id) {
        return OrderResponse.from(findOrder(id));
    }

    @Override
    public List<OrderResponse> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(OrderResponse::from)
                .toList();
    }

    @Override
    @Transactional
    public OrderResponse completeOrder(Long orderId) {
        Order order = findOrder(orderId);
        order.setStatus(OrderStatus.COMPLETED);
        order.setFailureReason(null);
        log.info("Order id={} COMPLETED", orderId);
        return OrderResponse.from(orderRepository.save(order));
    }

    @Override
    @Transactional
    public OrderResponse cancelOrder(Long orderId, String reason) {
        Order order = findOrder(orderId);
        order.setStatus(OrderStatus.CANCELLED);
        order.setFailureReason(reason);
        log.error("Order id={} CANCELLED: {}", orderId, reason);
        return OrderResponse.from(orderRepository.save(order));
    }

    private Order findOrder(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy đơn hàng id=" + id));
    }
}
