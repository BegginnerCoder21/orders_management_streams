package com.learnkafkastreams.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import static com.learnkafkastreams.util.ProducerUtil.*;

@Slf4j
@Service
public class OrderService {

    private final OrderStoreService orderStoreService;

    public OrderService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public void getOrdersCount(String orderType) {
       var orderTypeCount =  this.getOrderTypeCount(orderType);
       log.info("orderTypeCount: {}", orderTypeCount);
    }

    public ReadOnlyKeyValueStore<String, Long> getOrderTypeCount(String storeName) {
        return switch (storeName) {
            case GENERAL_ORDERS -> this.orderStoreService.orderStoreCount(GENERAL_ORDERS_COUNT);
            case RESTAURANT_ORDERS -> this.orderStoreService.orderStoreCount(RESTAURANT_ORDERS_COUNT);
            default -> throw new IllegalStateException("Option de commande non valide");
        };
    }
}

