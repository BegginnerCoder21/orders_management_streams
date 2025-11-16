package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.service.OrderCountWindowService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1/orders/windows/")
public class OrdersWindowController {

    private final OrderCountWindowService orderCountWindowService;

    public OrdersWindowController(OrderCountWindowService orderCountWindowService) {
        this.orderCountWindowService = orderCountWindowService;
    }

    @GetMapping("count/{order_type}")
    public ResponseEntity<List<OrdersCountPerStoreByWindowsDTO>> getOrderCountWindow(@PathVariable("order_type") String orderType)
    {

        List<OrdersCountPerStoreByWindowsDTO> ordersCountPerStoreByWindowsDTOS = this.orderCountWindowService.getOrdersCountWindow(orderType);

        return ResponseEntity.ok(ordersCountPerStoreByWindowsDTOS);
    }
}
