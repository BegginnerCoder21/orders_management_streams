package com.learnkafkastreams.util;

import com.learnkafkastreams.domain.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Slf4j
public class OrderTimeStampExtractor implements TimestampExtractor {
    
    @Override
    public long extract(ConsumerRecord<Object, Object> records, long partitionTime) {

        Order order = (Order) records.value();

        if(order != null && order.orderedDateTime() != null)
        {
            LocalDateTime timeStamp = order.orderedDateTime();

            log.info("timeStamp in extractor: {}", timeStamp);

            long dateConverted = this.convertToInstantFromUTC(timeStamp);

            log.info("timeStamp converted : {}", dateConverted);

            return dateConverted;

        }

        return partitionTime;
    }

    private long convertToInstantFromUTC(LocalDateTime timeStamp) {

        return timeStamp.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

}