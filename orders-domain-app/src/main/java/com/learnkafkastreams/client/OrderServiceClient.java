package com.learnkafkastreams.client;

import com.learnkafkastreams.domain.HostInfoDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;

@Component
@Slf4j
public class OrderServiceClient {

    private final WebClient webClient;

    public OrderServiceClient(WebClient webClient) {
        this.webClient = webClient;
    }

    public List<OrderCountPerStoreDTO> retrieveOrdersCountByOrderType(String orderType, HostInfoDTO hostInfoDTO)
    {
        String basePath = "http://" + hostInfoDTO.host() + ":" + hostInfoDTO.port();
        String uri = UriComponentsBuilder
                .fromUriString(basePath)
                .path("/v1/orders/count/{order_type}")
                .queryParam("query_other_hosts", "false")
                .buildAndExpand(orderType)
                .toString();

        return webClient.get()
                .uri(uri)
                .retrieve()
                .bodyToFlux(OrderCountPerStoreDTO.class)
                .collectList()
                .block();
    }
}
