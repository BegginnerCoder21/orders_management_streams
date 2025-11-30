package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.learnkafkastreams.util.ProducerUtil.*;

@Component
@Slf4j
public class OrdersTopology {

    @Autowired
    public void ordersStream(StreamsBuilder streamsBuilder) {
        orderTopology(streamsBuilder);
    }

    public static void orderTopology(StreamsBuilder streamsBuilder) {

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

        var orderStreams = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Order.class))
                                .withTimestampExtractor(new OrderTimeStampExtractor())
                )
                .selectKey((key, value) -> value.locationId());

        var storesTable = streamsBuilder
                .table(STORES,
                        Consumed.with(Serdes.String(), new JsonSerde<Store>(Store.class)));

        storesTable
                .toStream()
                .print(Printed.<String, Store>toSysOut().withLabel(STORES));

        orderStreams
                .print(Printed.<String, Order>toSysOut().withLabel(ORDERS));

        orderStreams.split(Named.as("General-restaurant-stream"))
                .branch(
                        generalPredicate,
                        Branched.withConsumer(generalOrderStream -> {
                            generalOrderStream.print(Printed.<String, Order>toSysOut().withLabel("generalOrderStream"));

//                            generalOrderStream
//                                    .mapValues((readonlyKey, value) -> revenueValueMapper.apply(value))
//                                    .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdesFactory
//                                            .revenueSerdesUsingGeneric()));

//                            aggregateOrderByCount(generalOrderStream, GENERAL_ORDERS_COUNT);
                            aggregateOrdersByCount(generalOrderStream, GENERAL_ORDERS_COUNT, storesTable);
                            aggregateOrdersByCountTimeWindow(generalOrderStream, GENERAL_ORDERS_COUNT_WINDOWS, storesTable);
//                            aggregateOrderByRevenue(generalOrderStream, GENERAL_ORDERS_REVENUE);
                            aggregateOrderByRevenueWithAddress(generalOrderStream, GENERAL_ORDERS_REVENUE, storesTable);
                            aggregateOrderByRevenueWithAddressTimeWindow(generalOrderStream, GENERAL_ORDERS_REVENUE_WINDOWS, storesTable);
                        })
                ).branch(restaurantPredicate,
                        Branched.withConsumer(restaurantOrderStream -> {
                            restaurantOrderStream
                                    .print(Printed.<String, Order>toSysOut()
                                            .withLabel("restaurantOrderStream"));

//                            restaurantOrderStream
//                                    .mapValues((readonlyKey, value) -> revenueValueMapper.apply(value))
//                                    .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdesFactory
//                                            .revenueSerdesUsingGeneric()));
//                            aggregateOrderByCount(restaurantOrderStream, RESTAURANT_ORDERS_COUNT);
                            aggregateOrdersByCount(restaurantOrderStream, RESTAURANT_ORDERS_COUNT, storesTable);
//                            aggregateOrderByRevenue(restaurantOrderStream, RESTAURANT_ORDERS_REVENUE);
                            aggregateOrdersByCountTimeWindow(restaurantOrderStream, RESTAURANT_ORDERS_COUNT_WINDOWS, storesTable);
                            aggregateOrderByRevenueWithAddress(restaurantOrderStream, RESTAURANT_ORDERS_REVENUE, storesTable);
                            aggregateOrderByRevenueWithAddressTimeWindow(restaurantOrderStream, RESTAURANT_ORDERS_REVENUE_WINDOWS, storesTable);

                        })
                );

    }

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrdersStream, String storeName, KTable<String, Store> storesTable) {

        var ordersCountPerStore = generalOrdersStream
                //.map((key, value) -> KeyValue.pair(value.locationId(), value) )
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<Order>(Order.class)))
                .count(Named.as(storeName), Materialized.as(storeName));

        ordersCountPerStore
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(storeName));


        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;

        var revenueWithStoreTable = ordersCountPerStore
                .join(storesTable, valueJoiner);

        revenueWithStoreTable
                .toStream()
                .print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel(storeName + "-by-store"));
    }

    private static void aggregateOrdersByCountTimeWindow(KStream<String, Order> generalOrderStream, String storeName, KTable<String, Store> storeTable) {

        Duration duration = Duration.ofSeconds(60);
        Duration graceDuration = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(duration, graceDuration);
        var ordersCountPerStore = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value) )
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<Order>(Order.class)))
                .windowedBy(timeWindows)
                .count(Named.as(storeName), Materialized.as(storeName))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        ordersCountPerStore
                .toStream()
                .peek((key, value) -> {
                    log.info("StoreName: {}, Key: {}, Value: {} ", storeName, key, value);
                    printLocalDateTimes(key,value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(storeName));


//        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;

//        var revenueWithStoreTable = ordersCountPerStore
//                .join(storeTable, valueJoiner);
//
//        revenueWithStoreTable
//                .toStream()
//                .print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel(storeName + "-by-store"));
    }

    private static void printLocalDateTimes(Windowed<String> key, Object value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();

        // Log brut UTC
        log.info("UTC startTime : {} , UTC endTime : {}, Count : {}", startTime, endTime, value);

        // Conversion UTC → Local (ex. Africa/Abidjan)
        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of("Africa/Abidjan"));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of("Africa/Abidjan"));

        log.info("Local startLDT : {} , Local endLDT : {}, Count : {}", startLDT, endLDT, value);
    }

    private static void aggregateOrderByRevenue(KStream<String, Order> orderStream, String storeName) {

        Initializer<TotalRevenue> initializerTotalRevenue = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator = (key, value, aggregate) -> aggregate.updateRunningRevenue(key, value);

        KTable<String, TotalRevenue> aggregateTotalRevenue = orderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<Order>(Order.class)))
                .aggregate(initializerTotalRevenue, aggregator, Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(storeName)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new JsonSerde<TotalRevenue>(TotalRevenue.class))
                );

        aggregateTotalRevenue
                .toStream()
                .print(Printed.<String, TotalRevenue>toSysOut()
                        .withLabel(storeName));

    }

    private static void aggregateOrderByRevenueWithAddress(KStream<String, Order> orderStream, String storeName, KTable<String, Store> storeTable) {

        Initializer<TotalRevenue> initializerTotalRevenue = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator = (key, value, aggregate) -> aggregate.updateRunningRevenue(key, value);

        KTable<String, TotalRevenue> aggregateTotalRevenue = orderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<Order>(Order.class)))
                .aggregate(initializerTotalRevenue, aggregator, Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(storeName)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new JsonSerde<TotalRevenue>(TotalRevenue.class))
                );

        aggregateTotalRevenue
                .toStream()
                .print(Printed.<String, TotalRevenue>toSysOut()
                        .withLabel(storeName));

        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        KTable<String, TotalRevenueWithAddress> totalRevenueWithAddressJoin = aggregateTotalRevenue.join(storeTable, valueJoiner);

        totalRevenueWithAddressJoin
                .toStream()
                .print(Printed.<String, TotalRevenueWithAddress>toSysOut()
                        .withLabel(storeName + "-by-store"));

    }

    private static void aggregateOrderByRevenueWithAddressTimeWindow(KStream<String, Order> generalOrderStream, String storeName, KTable<String, Store> storeTable) {

        Initializer<TotalRevenue> initializerTotalRevenue = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator = (key, value, aggregate) -> aggregate.updateRunningRevenue(key, value);

        Duration duration = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(duration);

        KTable<Windowed<String>, TotalRevenue> aggregateTotalRevenue = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<Order>(Order.class)))
                .windowedBy(timeWindows)
                .aggregate(initializerTotalRevenue, aggregator, Materialized.<String, TotalRevenue, WindowStore<Bytes, byte[]>>as(storeName)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new JsonSerde<TotalRevenue>(TotalRevenue.class))
                );

        aggregateTotalRevenue
                .toStream()
                .peek((key, value) -> {
                    log.info("StoreName: {}, Key: {}, Value: {} ", storeName, key, value);
                    printLocalDateTimes(key,value);
                })
                .print(Printed.<Windowed<String>, TotalRevenue>toSysOut()
                        .withLabel(storeName));

        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        var joinedParams = Joined.with(Serdes.String(), new JsonSerde<TotalRevenue>(TotalRevenue.class), new JsonSerde<Store>(Store.class));

        KStream<String, TotalRevenueWithAddress> totalRevenueWithAddressJoin = aggregateTotalRevenue
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .join(storeTable, valueJoiner, joinedParams);


        totalRevenueWithAddressJoin
                .print(Printed.<String, TotalRevenueWithAddress>toSysOut()
                        .withLabel(storeName + "-by-store"));
    }
}
