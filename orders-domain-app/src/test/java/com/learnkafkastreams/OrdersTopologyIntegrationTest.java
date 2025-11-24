package com.learnkafkastreams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.apache.kafka.streams.KeyValue;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static com.learnkafkastreams.util.ProducerUtil.*;

@SpringBootTest
@EmbeddedKafka(topics = {ORDERS, STORES})
@TestPropertySource(
        properties = {
                "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        }
)
public class OrdersTopologyIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(OrdersTopologyIntegrationTest.class);

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    ObjectMapper objectMapper;

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
        var keyValue1 = org.apache.kafka.streams.KeyValue.pair(order1.locationId(), order1);

        var order2 = new Order(54321, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
//                LocalDateTime.now()
                LocalDateTime.parse("2025-11-23T11:10:01")
                //LocalDateTime.now(ZoneId.of("UTC"))
        );
        var keyValue2 = org.apache.kafka.streams.KeyValue.pair(order2.locationId(), order2);

        var order3 = new Order(12345, "store_4567",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
//                LocalDateTime.now(),
                LocalDateTime.parse("2025-11-23T11:10:01")
                //LocalDateTime.now(ZoneId.of("UTC"))
        );
        var keyValue3 = KeyValue.pair(order3.locationId(), order3);

        var order4 = new Order(12345, "store_4567",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItems,
//                LocalDateTime.now()
                LocalDateTime.parse("2025-11-23T11:10:01")
                //LocalDateTime.now(ZoneId.of("UTC"))
        );
        var keyValue4 = org.apache.kafka.streams.KeyValue.pair(order4.locationId(), order4);

        return List.of(
                keyValue1,
                keyValue2
//                keyValue3,
//                keyValue4
        );
    }
}
