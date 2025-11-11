package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.Store;
import com.learnkafkastreams.util.OrderTimeStampExtractor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;

import static com.learnkafkastreams.util.ProducerUtil.ORDERS;
import static com.learnkafkastreams.util.ProducerUtil.STORES;

public class OrdersTopology {

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {

        orderTopology(streamsBuilder);

    }

    private static void orderTopology(StreamsBuilder streamsBuilder) {
        var orderStreams = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Order.class))
                                .withTimestampExtractor(new OrderTimeStampExtractor())
                )
                .selectKey((key, value) -> value.locationId());

        var storesTable = streamsBuilder
                .table(STORES,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Store.class)));

        storesTable
                .toStream()
                .print(Printed.<String, Store>toSysOut().withLabel(STORES));

        orderStreams
                .print(Printed.<String, Order>toSysOut().withLabel(ORDERS));


    }
}
