package com.learnkafkastreams.service;

import com.learnkafkastreams.client.OrderServiceClient;
import com.learnkafkastreams.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
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
    private final OrderServiceClient orderServiceClient;
    @Value("${server.port}")
    private Integer port;

    public OrderCountService(OrderStoreService orderStoreService, MetaDataService metaDataService, OrderServiceClient orderServiceClient) {
        this.orderStoreService = orderStoreService;
        this.metaDataService = metaDataService;
        this.orderServiceClient = orderServiceClient;
    }

    public List<OrderCountPerStoreDTO> getOrdersCount(String orderType, String queryOtherHosts) {
        ReadOnlyKeyValueStore<String, Long> orderTypeCount =  this.getOrderTypeCount(orderType);

        KeyValueIterator<String, Long> orders = orderTypeCount.all();

       Spliterator<KeyValue<String, Long>> spliterator = Spliterators.spliteratorUnknownSize(orders, 0);
       List<OrderCountPerStoreDTO> orderCountPerStoreDTOListCurrent = StreamSupport
                .stream(spliterator, false)
                .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                .toList();

       boolean convertQueryOtherHostsInBoolean = Boolean.parseBoolean(queryOtherHosts);
       List<OrderCountPerStoreDTO> orderCountPerStoreDTOList = this.retrieveDataFromOtherInstances(orderType, convertQueryOtherHostsInBoolean);

       log.info("orderCountPerStoreDTOListCurrent : {}, orderCountPerStoreDTOList: {}", orderCountPerStoreDTOListCurrent, orderCountPerStoreDTOList);

       return Stream.of(orderCountPerStoreDTOListCurrent, orderCountPerStoreDTOList)
               .filter(Objects::nonNull)
               .flatMap(Collection::stream)
               .toList();
    }

    private List<OrderCountPerStoreDTO> retrieveDataFromOtherInstances(String orderType, boolean queryOtherHosts)
    {
        List<HostInfoDTO> otherHostsList = this.otherHosts();
        log.info("otherHostsList: {} ", otherHostsList);
        if(queryOtherHosts && otherHostsList != null && !otherHostsList.isEmpty())
        {
            return otherHostsList
                    .stream()
                    .map(hostInfo -> this.orderServiceClient.retrieveOrdersCountByOrderType(orderType, hostInfo))
                    .flatMap(Collection::stream)
                    .toList();
        }
        return Collections.emptyList();
    }

    private List<HostInfoDTO> otherHosts() {

       return metaDataService.getStreamMetaData()
                .stream()
                .filter((hostInfoDTO -> hostInfoDTO.port() != port))
                .toList();
    }

    public OrderCountPerStoreDTO getOrderCountByLocationId(String orderType, String locationId)
    {
        String storeName = this.mapOrderCountStoreName(orderType);

        HostInfoDTOWithKey hostInfoDTOWithKey =  this.metaDataService.getStreamMetaData(storeName, locationId);

        log.info("hostInfoDTOWithKey: {}", hostInfoDTOWithKey);

        if(hostInfoDTOWithKey == null) return null;

        if(hostInfoDTOWithKey.port() == port)
        {
            log.info("Requête de donnée sur l'instance courante");
            ReadOnlyKeyValueStore<String, Long> orderTypeCount =  this.getOrderTypeCount(orderType);

            Long orderCountReceived = orderTypeCount.get(locationId);

            if(orderCountReceived == null) return null;

            return new OrderCountPerStoreDTO(locationId, orderCountReceived);
        }

        log.info("Requête de donnée sur l'instance distant");

        HostInfoDTO hostInfoDTO = new HostInfoDTO(hostInfoDTOWithKey.host(), hostInfoDTOWithKey.port());

        //appel https vers instance qui contient la donnée
        return this.orderServiceClient.retrieveOrdersCountByOrderTypeAndLocaltionId(hostInfoDTO, locationId, orderType);
    }

    public String mapOrderCountStoreName(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> GENERAL_ORDERS_COUNT;
            case RESTAURANT_ORDERS -> RESTAURANT_ORDERS_COUNT;
            default -> throw new IllegalStateException("Option de commande non valide");
        };
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

        List<AllOrdersCountPerStoreDTO> generalOrderCount =  this.getOrdersCount(GENERAL_ORDERS, "false")
                .stream()
                .map((orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, GENERAL)))
                .toList();

        List<AllOrdersCountPerStoreDTO> restaurantOrderCount =  this.getOrdersCount(RESTAURANT_ORDERS, "false")
                .stream()
                .map((orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, RESTAURANT)))
                .toList();

        return Stream.of(generalOrderCount, restaurantOrderCount).flatMap(Collection::stream).toList();

    }
}

