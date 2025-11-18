package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.domain.OrdersRevenuePerStoreByWindowsDTO;
import com.learnkafkastreams.service.OrderCountWindowService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
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

    @GetMapping("count/")
    public ResponseEntity<List<OrdersCountPerStoreByWindowsDTO>> getAllOrderCountWindow(
            @RequestParam(value = "from_time", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime fromTime,
            @RequestParam(value = "to_time", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime toTime
            )
    {

        if(fromTime != null && toTime != null)
        {
            List<OrdersCountPerStoreByWindowsDTO> ordersCountPerStoreByWindowsDTOS =  this.orderCountWindowService.getAllOrdersCountWindow(fromTime, toTime);

            return ResponseEntity.ok(ordersCountPerStoreByWindowsDTOS);
        }

        List<OrdersCountPerStoreByWindowsDTO> ordersCountPerStoreByWindowsDTOS = this.orderCountWindowService.getAllOrdersCountWindow();

        return ResponseEntity.ok(ordersCountPerStoreByWindowsDTOS);
    }

    @GetMapping("revenue/{order_type}")
    public ResponseEntity<List<OrdersRevenuePerStoreByWindowsDTO>> getOrderRevenueWindow(@PathVariable("order_type") String orderType)
    {

        List<OrdersRevenuePerStoreByWindowsDTO> ordersCountPerStoreByWindowsDTOS = this.orderCountWindowService.getOrdersRevenueWindow(orderType);
        log.info("ordersCountPerStoreByWindowsDTOS: {}", ordersCountPerStoreByWindowsDTOS);
        return ResponseEntity.ok(ordersCountPerStoreByWindowsDTOS);
    }
}
