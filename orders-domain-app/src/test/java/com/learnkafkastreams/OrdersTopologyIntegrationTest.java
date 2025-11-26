package com.learnkafkastreams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.service.OrderCountService;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.apache.kafka.streams.KeyValue;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.learnkafkastreams.util.ProducerUtil.*;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(classes = OrdersDomainAppApplication.class)
@EmbeddedKafka(topics = {ORDERS, STORES})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestPropertySource(
        properties = {
                "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        }
)
class OrdersTopologyIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(OrdersTopologyIntegrationTest.class);

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    OrderCountService orderCountService;

    @BeforeEach
    void setUp()
    {
        streamsBuilderFactoryBean.start();
    }

    @AfterEach
    void tearDown() {
        Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams()).close();
        streamsBuilderFactoryBean.getKafkaStreams().cleanUp();
    }

    @Test
    void orderCount()
    {
        //given
        publishOrders();

        //when

        //then
        Awaitility
                .await()
                .atMost(10, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until( ()-> {
                    var generalGeneralCountStore = this.orderCountService.getOrdersCount(GENERAL_ORDERS);
                    log.info("general_orders_count_test: {}", generalGeneralCountStore);
                    return generalGeneralCountStore.size();
                }, equalTo(1));

        var generalOrderCountStore = this.orderCountService.getOrdersCount(GENERAL_ORDERS);

        assertEquals(1, generalOrderCountStore.getFirst().orderCount());
        assertEquals("store_1234", generalOrderCountStore.getFirst().locationId());


    }

    private void publishOrders()
    {
        orders()
                .forEach(order -> {
                    String ordersJson = null;
                    try {
                        ordersJson = objectMapper.writeValueAsString(order.value);
                        log.info("Published the order message : {} ", ordersJson);
                    } catch (JsonProcessingException e) {
                        log.error("JsonProcessingException : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    } catch (Exception e) {
                        log.error("Exception : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    }

                    kafkaTemplate.send(ORDERS, order.key, ordersJson);
                });
    }

    private static List<KeyValue<String, Order>> orders() {

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