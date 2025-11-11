package com.learnkafkastreams.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;

@Slf4j
public class StreamsProductionExceptionHandler implements ProductionExceptionHandler {

    @Override
    public ProductionExceptionHandlerResponse handle(ErrorHandlerContext context, ProducerRecord<byte[], byte[]> record, Exception exception) {

        log.info("Serialization Exception is : {}, and the kafka record is : {}", exception.getMessage(), record, exception);

        return ProductionExceptionHandlerResponse.CONTINUE;
    }
    @Override
    public void configure(Map<String, ?> map) {

    }

}
