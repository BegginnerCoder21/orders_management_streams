package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.domain.OrderType.GENERAL;
import static com.learnkafkastreams.domain.OrderType.RESTAURANT;
import static com.learnkafkastreams.util.ProducerUtil.*;

@Slf4j
@Service
public class OrderCountService {

    private final OrderStoreService orderStoreService;
    private final MetaDataService metaDataService;
    @Value("${server.port}")
    private Integer port;

    public OrderCountService(OrderStoreService orderStoreService, MetaDataService metaDataService) {
        this.orderStoreService = orderStoreService;
        this.metaDataService = metaDataService;
    }

    public List<OrderCountPerStoreDTO> getOrdersCount(String orderType) {
        ReadOnlyKeyValueStore<String, Long> orderTypeCount =  this.getOrderTypeCount(orderType);

        KeyValueIterator<String, Long> orders = orderTypeCount.all();

       Spliterator<KeyValue<String, Long>> spliterator = Spliterators.spliteratorUnknownSize(orders, 0);

       this.retrieveDataFromOtherInstances(orderType);
       return StreamSupport
               .stream(spliterator, false)
               .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
               .toList();
    }

    private void retrieveDataFromOtherInstances(String orderType)
    {
        List<HostInfoDTO> otherHostsList = this.otherHosts();
        log.info("otherHostsList: {} ", otherHostsList);
        if(otherHostsList.isEmpty())
        {

        }
    }

    private List<HostInfoDTO> otherHosts() {

       return metaDataService.getStreamMetaData()
                .stream()
                .filter((hostInfoDTO -> hostInfoDTO.port() != port))
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
                .map((orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, GENERAL)))
                .toList();

        List<AllOrdersCountPerStoreDTO> restaurantOrderCount =  this.getOrdersCount(RESTAURANT_ORDERS)
                .stream()
                .map((orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, RESTAURANT)))
                .toList();

        return Stream.of(generalOrderCount, restaurantOrderCount).flatMap(Collection::stream).toList();

    }
}

