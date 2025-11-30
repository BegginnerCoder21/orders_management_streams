package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.HostInfoDTO;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class MetaDataService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public MetaDataService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public List<HostInfoDTO> getStreamMetaData() {

        var kafkaStreams = this.streamsBuilderFactoryBean.getKafkaStreams();

        if (kafkaStreams == null) {
            return List.of();
        }

        return kafkaStreams
                .metadataForAllStreamsClients()
                .stream()
                .map(meta -> {
                    HostInfo hostInfo = meta.hostInfo();
                    return new HostInfoDTO(hostInfo.host(), hostInfo.port());
                })
                .toList();
    }
}
