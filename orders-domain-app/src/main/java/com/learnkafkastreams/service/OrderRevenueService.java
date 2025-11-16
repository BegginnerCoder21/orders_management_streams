package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.domain.OrderType.GENERAL;
import static com.learnkafkastreams.domain.OrderType.RESTAURANT;
import static com.learnkafkastreams.util.ProducerUtil.*;

@Slf4j
@Service
public class OrderRevenueService {

    private final OrderStoreService orderStoreService;

    public OrderRevenueService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public ReadOnlyKeyValueStore<String, TotalRevenue> getOrderTypeRevenue(String storeName) {
        return switch (storeName) {
            case GENERAL_ORDERS -> this.orderStoreService.orderStoreRevenue(GENERAL_ORDERS_REVENUE);
            case RESTAURANT_ORDERS -> this.orderStoreService.orderStoreRevenue(RESTAURANT_ORDERS_REVENUE);
            default -> throw new IllegalStateException("Option de commande non valide");
        };
    }

    public List<OrderRevenueDTO> getOrdersRevenue(String orderType) {
        ReadOnlyKeyValueStore<String, TotalRevenue> orderTypeRevenue =  this.getOrderTypeRevenue(orderType);

        KeyValueIterator<String, TotalRevenue> orders = orderTypeRevenue.all();

        Spliterator<KeyValue<String, TotalRevenue>> spliterator = Spliterators.spliteratorUnknownSize(orders, 0);

        return StreamSupport
                .stream(spliterator, false)
                .map(keyValue -> new OrderRevenueDTO(keyValue.key, mapOrderType(orderType), keyValue.value))
                .toList();
    }

    public static OrderType mapOrderType(String orderType) {

        return switch (orderType) {
            case GENERAL_ORDERS -> GENERAL;
            case RESTAURANT_ORDERS -> RESTAURANT;
            default -> throw new IllegalStateException("Option de commande non valide");
        };
    }
}

