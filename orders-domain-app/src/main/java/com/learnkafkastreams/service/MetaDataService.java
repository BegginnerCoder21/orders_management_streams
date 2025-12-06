package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.HostInfoDTO;
import com.learnkafkastreams.domain.HostInfoDTOWithKey;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyQueryMetadata;
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

    public HostInfoDTOWithKey getStreamMetaData(String storeName, String locationId) {

        var kafkaStreams = this.streamsBuilderFactoryBean.getKafkaStreams();

        if (kafkaStreams == null) return null;

        KeyQueryMetadata metadataForKey =  kafkaStreams
                .queryMetadataForKey(storeName, locationId, Serdes.String().serializer());

        if(metadataForKey == null) return null;

        var activeHost = metadataForKey.activeHost();
        return new HostInfoDTOWithKey(activeHost.host(), activeHost.port(), locationId);
    }
}
