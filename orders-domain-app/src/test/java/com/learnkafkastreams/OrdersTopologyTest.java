package com.learnkafkastreams;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.TotalRevenue;
import com.learnkafkastreams.topology.OrdersTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static com.learnkafkastreams.util.ProducerUtil.*;
import static org.junit.jupiter.api.Assertions.assertEquals;


class OrdersTopologyTest {

    private static final Logger log = LoggerFactory.getLogger(OrdersTopologyTest.class);
    private TopologyTestDriver topologyTestDriver = null;
    private TestInputTopic<String, Order> testInputTopic = null;
    static String INPUT_TOPIC = ORDERS;
    OrdersTopology ordersTopology = new OrdersTopology();
    StreamsBuilder streamsBuilder = null;

    @BeforeEach
    void setUp() {

        streamsBuilder = new StreamsBuilder();
        ordersTopology.orderTopology(streamsBuilder);

        topologyTestDriver = new TopologyTestDriver(streamsBuilder.build());

        testInputTopic = topologyTestDriver.
                createInputTopic(INPUT_TOPIC,
                        Serdes.String().serializer(),
                        new JsonSerde<>(Order.class).serializer());

    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void orderCount() {

        //given
        testInputTopic.pipeKeyValueList(buildOrders());

        //when
        ReadOnlyKeyValueStore<String, Long> generalOrdersCountStore = topologyTestDriver.getKeyValueStore(GENERAL_ORDERS_COUNT);
        var generalOrderCount =  generalOrdersCountStore.get("store_1234");
        assertEquals(1, generalOrderCount);

        ReadOnlyKeyValueStore<String, Long>  restaurantOrdersCountStore = topologyTestDriver.getKeyValueStore(RESTAURANT_ORDERS_COUNT);
        var restaurantOrderCount = restaurantOrdersCountStore.get("store_1234");
        assertEquals(1, restaurantOrderCount);

    }

    @Test
    void orderRevenue() {

        //given
        testInputTopic.pipeKeyValueList(buildOrders());

        //when
        ReadOnlyKeyValueStore<String, TotalRevenue> generalOrdersRevenueStore = topologyTestDriver.getKeyValueStore(GENERAL_ORDERS_REVENUE);
        var generalOrderCount =  generalOrdersRevenueStore.get("store_1234");
        assertEquals(1, generalOrderCount.runnuingOrderCount());
        assertEquals(new BigDecimal("27.00"), generalOrderCount.runningRevenue());

        ReadOnlyKeyValueStore<String, TotalRevenue>  restaurantOrdersRevenueStore = topologyTestDriver.getKeyValueStore(RESTAURANT_ORDERS_REVENUE);
        var restaurantOrderCount = restaurantOrdersRevenueStore.get("store_1234");
        assertEquals(1, restaurantOrderCount.runnuingOrderCount());
        assertEquals(new BigDecimal("15.00"), restaurantOrderCount.runningRevenue());

    }

    @Test
    void orderRevenue_multipleOrders() {

        //given
        testInputTopic.pipeKeyValueList(buildOrders());
        testInputTopic.pipeKeyValueList(buildOrders());

        //when
        ReadOnlyKeyValueStore<String, TotalRevenue> generalOrdersRevenueStore = topologyTestDriver.getKeyValueStore(GENERAL_ORDERS_REVENUE);
        var generalOrderCount =  generalOrdersRevenueStore.get("store_1234");
        assertEquals(2, generalOrderCount.runnuingOrderCount());
        assertEquals(new BigDecimal("54.00"), generalOrderCount.runningRevenue());

        ReadOnlyKeyValueStore<String, TotalRevenue>  restaurantOrdersRevenueStore = topologyTestDriver.getKeyValueStore(RESTAURANT_ORDERS_REVENUE);
        var restaurantOrderCount = restaurantOrdersRevenueStore.get("store_1234");
        assertEquals(2, restaurantOrderCount.runnuingOrderCount());
        assertEquals(new BigDecimal("30.00"), restaurantOrderCount.runningRevenue());

    }

    @Test
    void orderRevenue_byWindows() {

        //given
        testInputTopic.pipeKeyValueList(buildOrders());
        testInputTopic.pipeKeyValueList(buildOrders());

        //when
        WindowStore<String, TotalRevenue> generalOrdersRevenueStore = topologyTestDriver.getWindowStore(GENERAL_ORDERS_REVENUE_WINDOWS);

        generalOrdersRevenueStore.all().forEachRemaining(windowedTotalRevenueKeyValue -> {

            Instant startTime = windowedTotalRevenueKeyValue.key.window().startTime();
            Instant endTime = windowedTotalRevenueKeyValue.key.window().endTime();

            LocalDateTime expectedStartValue = LocalDateTime.parse("2025-11-23T11:10:00");
            LocalDateTime expectedEndValue = LocalDateTime.parse("2025-11-23T11:10:15");

            assert LocalDateTime.ofInstant(startTime, ZoneId.of("Africa/Abidjan")).equals(expectedStartValue);
            assert LocalDateTime.ofInstant(endTime, ZoneId.of("Africa/Abidjan")).equals(expectedEndValue);
            log.info("key store general: {}", windowedTotalRevenueKeyValue.key.key());
            var totalRevenue = windowedTotalRevenueKeyValue.value;
            assertEquals(2, totalRevenue.runnuingOrderCount());
            assertEquals(new BigDecimal("54.00"), totalRevenue.runningRevenue());

        });


        WindowStore<String, TotalRevenue>  restaurantOrdersRevenueStore = topologyTestDriver.getWindowStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);

        restaurantOrdersRevenueStore.all().forEachRemaining(windowedTotalRevenueKeyValue -> {

            Instant startTime = windowedTotalRevenueKeyValue.key.window().startTime();
            Instant endTime = windowedTotalRevenueKeyValue.key.window().endTime();

            LocalDateTime expectedStartValue = LocalDateTime.parse("2025-11-23T11:10:00");
            LocalDateTime expectedEndValue = LocalDateTime.parse("2025-11-23T11:10:15");

            assert LocalDateTime.ofInstant(startTime, ZoneId.of("Africa/Abidjan")).equals(expectedStartValue);
            assert LocalDateTime.ofInstant(endTime, ZoneId.of("Africa/Abidjan")).equals(expectedEndValue);

            var totalRevenue = windowedTotalRevenueKeyValue.value;
            log.info("key store restaurant: {}", windowedTotalRevenueKeyValue.key.key());
            assertEquals(2, totalRevenue.runnuingOrderCount());
            assertEquals(new BigDecimal("30.00"), totalRevenue.runningRevenue());

        });

    }

    private static List<KeyValue<String, Order>> buildOrders() {
        var orderItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var orderItemsRestaurant = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new Order(12345, "store_1234",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
//                LocalDateTime.now()
                LocalDateTime.parse("2025-11-23T11:10:01")
                //LocalDateTime.now(ZoneId.of("UTC"))
        );
        var keyValue1 = KeyValue.pair(order1.orderId().toString(), order1);

        var order2 = new Order(54321, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
//                LocalDateTime.now()
                LocalDateTime.parse("2025-11-23T11:10:01")
                //LocalDateTime.now(ZoneId.of("UTC"))
        );
        var keyValue2 = KeyValue.pair(order2.orderId().toString(), order2);

        return List.of(
                keyValue1,
                keyValue2
        );
    }
}
