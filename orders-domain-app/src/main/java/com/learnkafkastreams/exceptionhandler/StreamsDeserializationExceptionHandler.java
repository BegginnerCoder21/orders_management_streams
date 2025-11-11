package com.learnkafkastreams.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;

import java.util.Map;

@Slf4j
public class StreamsDeserializationExceptionHandler implements DeserializationExceptionHandler {

    int errorCounter =0;
    @Override
    public DeserializationHandlerResponse handle(ErrorHandlerContext errorHandlerContext, ConsumerRecord<byte[], byte[]> consumerRecord, Exception e) {

        log.info("Exception is : {} and the kafka record is : {}", e.getMessage(), consumerRecord, e);
        log.info("errorCounter : {}", errorCounter);

        if(errorCounter < 2){
            errorCounter++;
            log.info("errorCounter1 : {}", errorCounter);
            return DeserializationHandlerResponse.CONTINUE;
        }
        log.info("errorCounter2 : {}", errorCounter);
        return DeserializationHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
