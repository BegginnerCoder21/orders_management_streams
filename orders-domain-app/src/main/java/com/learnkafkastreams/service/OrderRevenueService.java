package com.learnkafkastreams.service;

import com.learnkafkastreams.client.OrderServiceClient;
import com.learnkafkastreams.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.domain.OrderType.GENERAL;
import static com.learnkafkastreams.domain.OrderType.RESTAURANT;
import static com.learnkafkastreams.util.ProducerUtil.*;

@Slf4j
@Service
public class OrderRevenueService {

    private final OrderStoreService orderStoreService;
    private final OrderServiceClient orderServiceClient;
    private final OrderCountService orderCountService;

    public OrderRevenueService(OrderStoreService orderStoreService, OrderServiceClient orderServiceClient, OrderCountService orderCountService) {
        this.orderStoreService = orderStoreService;
        this.orderServiceClient = orderServiceClient;
        this.orderCountService = orderCountService;
    }

    public ReadOnlyKeyValueStore<String, TotalRevenue> getOrderTypeRevenue(String storeName) {
        return switch (storeName) {
            case GENERAL_ORDERS -> this.orderStoreService.orderStoreRevenue(GENERAL_ORDERS_REVENUE);
            case RESTAURANT_ORDERS -> this.orderStoreService.orderStoreRevenue(RESTAURANT_ORDERS_REVENUE);
            default -> throw new IllegalStateException("Option de commande non valide");
        };
    }

    public List<OrderRevenueDTO> getOrdersRevenue(String orderType, String queryOtherHosts) {
        ReadOnlyKeyValueStore<String, TotalRevenue> orderTypeRevenue =  this.getOrderTypeRevenue(orderType);

        KeyValueIterator<String, TotalRevenue> orders = orderTypeRevenue.all();

        Spliterator<KeyValue<String, TotalRevenue>> spliterator = Spliterators.spliteratorUnknownSize(orders, 0);

        List<OrderRevenueDTO> ordersCountRevenueCurrent = StreamSupport
                .stream(spliterator, false)
                .map(keyValue -> new OrderRevenueDTO(keyValue.key, mapOrderType(orderType), keyValue.value))
                .toList();


        boolean convertQueryOtherHostsInBoolean = Boolean.parseBoolean(queryOtherHosts);
        List<OrderRevenueDTO> ordersCountRevenueList = this.retrieveRevenueDataFromOtherInstances(orderType, convertQueryOtherHostsInBoolean);

        log.info("ordersCountRevenueCurrent : {}, ordersCountRevenueList: {}", ordersCountRevenueCurrent, ordersCountRevenueList);

        return Stream.of(ordersCountRevenueCurrent, ordersCountRevenueList)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .toList();
    }

    private List<OrderRevenueDTO> retrieveRevenueDataFromOtherInstances(String orderType, boolean queryOtherHosts) {

        List<HostInfoDTO> otherHostsList = this.orderCountService.otherHosts();
        log.info("otherHostsList: {} ", otherHostsList);
        if(queryOtherHosts && otherHostsList != null && !otherHostsList.isEmpty())
        {
            return otherHostsList
                    .stream()
                    .map(hostInfo -> this.orderServiceClient.retrieveOrdersCountRevenueByOrderType(orderType, hostInfo))
                    .flatMap(Collection::stream)
                    .toList();
        }
        return Collections.emptyList();
    }

    public static OrderType mapOrderType(String orderType) {

        return switch (orderType) {
            case GENERAL_ORDERS -> GENERAL;
            case RESTAURANT_ORDERS -> RESTAURANT;
            default -> throw new IllegalStateException("Option de commande non valide");
        };
    }
}

