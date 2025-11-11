package com.learnkafkastreams.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

@Slf4j
public class StreamsProcessorCustomErrorHandler implements StreamsUncaughtExceptionHandler {

    @Override
    public StreamThreadExceptionResponse handle(Throwable throwable) {

        log.info("Exception is the application is : {}", throwable.getMessage());
        if(throwable instanceof StreamsException)
        {
            var cause = throwable.getCause();

            if(cause.getMessage().equals("Occurred Error")){

                return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }
        }
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    }
}
