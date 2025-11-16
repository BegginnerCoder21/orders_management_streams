package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.TotalRevenue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class OrderStoreService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public OrderStoreService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public ReadOnlyKeyValueStore<String, Long> orderStoreCount(String storeName) {

            return Objects.requireNonNull(this.streamsBuilderFactoryBean
                            .getKafkaStreams())
                    .store(StoreQueryParameters.
                            fromNameAndType(
                                    storeName,
                                    QueryableStoreTypes.keyValueStore()
                            )
                    );
    }

    public ReadOnlyWindowStore<String, Long> orderStoreCountWindow(String storeName) {

        return Objects.requireNonNull(this.streamsBuilderFactoryBean
                        .getKafkaStreams())
                .store(StoreQueryParameters.
                        fromNameAndType(
                                storeName,
                                QueryableStoreTypes.windowStore()
                        )
                );
    }

    public ReadOnlyKeyValueStore<String, TotalRevenue> orderStoreRevenue(String storeName) {

        return Objects.requireNonNull(this.streamsBuilderFactoryBean
                        .getKafkaStreams())
                .store(StoreQueryParameters.
                        fromNameAndType(
                                storeName,
                                QueryableStoreTypes.keyValueStore()
                        )
                );
    }
}
