package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderRevenueDTO;
import com.learnkafkastreams.service.OrderCountService;
import com.learnkafkastreams.service.OrderRevenueService;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/orders/")
public class OrdersController {

    private final OrderCountService orderCountService;
    private final OrderRevenueService orderRevenueService;

    public OrdersController(OrderCountService orderCountService, OrderRevenueService orderRevenueService) {
        this.orderCountService = orderCountService;
        this.orderRevenueService = orderRevenueService;
    }

    @GetMapping("count/{order_type}")
    public ResponseEntity<?> getOrderCount(@PathVariable("order_type") String orderType,
                                           @RequestParam(value = "location_id", required = false) String locationId,
                                           @RequestParam(value = "query_other_hosts", required = false) String queryOtherHosts)
    {

        if(!StringUtils.hasText(queryOtherHosts))
        {
            queryOtherHosts = "true";//pour faire un appel vers les autres instances
        }
        if(StringUtils.hasLength(locationId))
        {
            OrderCountPerStoreDTO orderCountPerStoreDTO = this.orderCountService.getOrderCountByLocationId(orderType, locationId);

            return ResponseEntity.ok(orderCountPerStoreDTO);
        }
        List<OrderCountPerStoreDTO> orderCountPerStoreList = this.orderCountService.getOrdersCount(orderType, queryOtherHosts);

        return ResponseEntity.ok(orderCountPerStoreList);
    }

    @GetMapping("revenue/{order_type}")
    public ResponseEntity<List<OrderRevenueDTO>> getOrderRevenue(@PathVariable("order_type") String orderType)
    {
        List<OrderRevenueDTO> ordersRevenue = this.orderRevenueService.getOrdersRevenue(orderType);

        return ResponseEntity.ok(ordersRevenue);
    }

    @GetMapping("count/")
    public ResponseEntity<List<AllOrdersCountPerStoreDTO>> getAllOrdersCount()
    {
        List<AllOrdersCountPerStoreDTO> allOrdersCountPerStoreDTOS =  this.orderCountService.getAllOrderCount();

        return ResponseEntity.ok(allOrdersCountPerStoreDTOS);
    }




}
