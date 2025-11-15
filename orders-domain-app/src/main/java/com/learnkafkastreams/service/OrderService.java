package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Stream;
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
        ReadOnlyKeyValueStore<String, Long> orderTypeCount =  this.getOrderTypeCount(orderType);

        KeyValueIterator<String, Long> orders = orderTypeCount.all();

       Spliterator<KeyValue<String, Long>> spliterator = Spliterators.spliteratorUnknownSize(orders, 2);

       return StreamSupport
               .stream(spliterator, false)
               .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
               .toList();
    }

    public OrderCountPerStoreDTO getOrderCountByLocationId(String orderType, String locationId)
    {
        ReadOnlyKeyValueStore<String, Long> orderTypeCount =  this.getOrderTypeCount(orderType);

        var orderCountReceived = orderTypeCount.get(locationId);

        return new OrderCountPerStoreDTO(locationId, orderCountReceived);

    }

    public ReadOnlyKeyValueStore<String, Long> getOrderTypeCount(String storeName) {
        return switch (storeName) {
            case GENERAL_ORDERS -> this.orderStoreService.orderStoreCount(GENERAL_ORDERS_COUNT);
            case RESTAURANT_ORDERS -> this.orderStoreService.orderStoreCount(RESTAURANT_ORDERS_COUNT);
            default -> throw new IllegalStateException("Option de commande non valide");
        };
    }

    public List<AllOrdersCountPerStoreDTO> getAllOrderCount() {

        BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStoreDTO> mapper =
                (orderCountPerStoreDTO, orderType) -> new AllOrdersCountPerStoreDTO(
                        orderCountPerStoreDTO.locationId(),
                        orderCountPerStoreDTO.orderCount(),
                        orderType);

        List<AllOrdersCountPerStoreDTO> generalOrderCount =  this.getOrdersCount(GENERAL_ORDERS)
                .stream()
                .map((orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.GENERAL)))
                .toList();

        List<AllOrdersCountPerStoreDTO> restaurantOrderCount =  this.getOrdersCount(RESTAURANT_ORDERS)
                .stream()
                .map((orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.RESTAURANT)))
                .toList();

        return Stream.of(generalOrderCount, restaurantOrderCount).flatMap(Collection::stream).toList();

    }
}

