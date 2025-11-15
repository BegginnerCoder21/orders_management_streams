package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/orders/")
public class OrdersController {

    private final OrderService orderService;

    public OrdersController(OrderService orderService) {
        this.orderService = orderService;
    }

    @GetMapping("count/{order_type}")
    public ResponseEntity<?> getOrderCount(@PathVariable("order_type") String orderType,
                                           @RequestParam(value = "location_id", required = false) String locationId)
    {
        if(StringUtils.hasLength(locationId))
        {
            OrderCountPerStoreDTO orderCountPerStoreDTO = this.orderService.getOrderCountByLocationId(orderType, locationId);

            return ResponseEntity.ok(orderCountPerStoreDTO);
        }
        List<OrderCountPerStoreDTO> orderCountPerStoreList = this.orderService.getOrdersCount(orderType);

        return ResponseEntity.ok(orderCountPerStoreList);
    }
}
