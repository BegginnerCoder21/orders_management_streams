package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.util.ProducerUtil.*;

@Slf4j
@Service
public class OrderService {

    private final OrderStoreService orderStoreService;

    public OrderService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrderCountPerStoreDTO> getOrdersCount(String orderType) {
       var orderTypeCount =  this.getOrderTypeCount(orderType);
       var orders = orderTypeCount.all();

       var spliterator = Spliterators.spliteratorUnknownSize(orders, 0);

       return StreamSupport
               .stream(spliterator, false)
               .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
               .toList();
    }

    public ReadOnlyKeyValueStore<String, Long> getOrderTypeCount(String storeName) {
        return switch (storeName) {
            case GENERAL_ORDERS -> this.orderStoreService.orderStoreCount(GENERAL_ORDERS_COUNT);
            case RESTAURANT_ORDERS -> this.orderStoreService.orderStoreCount(RESTAURANT_ORDERS_COUNT);
            default -> throw new IllegalStateException("Option de commande non valide");
        };
    }
}

