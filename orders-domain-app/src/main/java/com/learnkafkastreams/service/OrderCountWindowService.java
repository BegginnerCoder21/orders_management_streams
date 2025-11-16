package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.service.OrderRevenueService.mapOrderType;
import static com.learnkafkastreams.util.ProducerUtil.*;

@Service
public class OrderCountWindowService {

    private final OrderStoreService orderStoreService;

    public OrderCountWindowService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrdersCountPerStoreByWindowsDTO> getOrdersCountWindow(String orderType) {

        ReadOnlyWindowStore<String, Long> orderTypeCountWindow =  this.getOrderTypeCountWindow(orderType);

        KeyValueIterator<Windowed<String>, Long>  orders = orderTypeCountWindow.all();

        OrderType orderTypeWindow = mapOrderType(orderType);

        return this.mapToOrdersCountPerStoreByWindowsDTOS(orders, orderTypeWindow);
    }

    private List<OrdersCountPerStoreByWindowsDTO> mapToOrdersCountPerStoreByWindowsDTOS(KeyValueIterator<Windowed<String>, Long> orders, OrderType orderTypeWindow) {
        Spliterator<KeyValue<Windowed<String>, Long>> spliterator = Spliterators.spliteratorUnknownSize(orders, 0);


        return StreamSupport
                .stream(spliterator, false)
                .map(keyValue -> new OrdersCountPerStoreByWindowsDTO(
                        keyValue.key.key(),
                        keyValue.value,
                        orderTypeWindow,
                        LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")),
                        LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))
                ))
                .toList();
    }

    private ReadOnlyWindowStore<String, Long> getOrderTypeCountWindow(String storeName) {

        return switch (storeName) {
            case GENERAL_ORDERS -> this.orderStoreService.orderStoreCountWindow(GENERAL_ORDERS_COUNT_WINDOWS);
            case RESTAURANT_ORDERS -> this.orderStoreService.orderStoreCountWindow(RESTAURANT_ORDERS_COUNT_WINDOWS);
            default -> throw new IllegalStateException("Option de commande non valide");
        };
    }
    public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountWindow()
    {
        List<OrdersCountPerStoreByWindowsDTO> allGeneralOrdersCountWindow = this.getOrdersCountWindow(GENERAL_ORDERS);
        List<OrdersCountPerStoreByWindowsDTO> allRestaurantOrderCountWindow = this.getOrdersCountWindow(RESTAURANT_ORDERS);

        return Stream.of(allGeneralOrdersCountWindow, allRestaurantOrderCountWindow)
                .flatMap(Collection::stream)
                .toList();
    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountWindow(LocalDateTime fromTime, LocalDateTime toTime) {

        var fromTimeToInstant =  fromTime.toInstant(ZoneOffset.UTC);
        var toTimeToInstant = toTime.toInstant(ZoneOffset.UTC);

        KeyValueIterator<Windowed<String>, Long> allGeneralOrdersCountByWindow =  this.getOrderTypeCountWindow(GENERAL_ORDERS)
                .backwardFetchAll(fromTimeToInstant, toTimeToInstant);

        List<OrdersCountPerStoreByWindowsDTO> generalOrdersCountPerStoreByWindowsDTO =  this.mapToOrdersCountPerStoreByWindowsDTOS(allGeneralOrdersCountByWindow, OrderType.GENERAL);

        KeyValueIterator<Windowed<String>, Long> allRestaurantOrdersCountByWindow =  this.getOrderTypeCountWindow(RESTAURANT_ORDERS)
                .backwardFetchAll(fromTimeToInstant, toTimeToInstant);

        List<OrdersCountPerStoreByWindowsDTO> restaurantOrdersCountPerStoreByWindowDTO = this.mapToOrdersCountPerStoreByWindowsDTOS(allRestaurantOrdersCountByWindow, OrderType.RESTAURANT);

        return Stream.of(generalOrdersCountPerStoreByWindowsDTO, restaurantOrdersCountPerStoreByWindowDTO)
                .flatMap(Collection::stream)
                .toList();
    }
}
