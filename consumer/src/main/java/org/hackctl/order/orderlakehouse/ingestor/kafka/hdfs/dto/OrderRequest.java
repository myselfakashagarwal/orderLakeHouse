package org.hackctl.order.orderlakehouse.ingestor.kafka.hdfs.dto;

import java.util.List;
import lombok.Data;

@Data
public class OrderRequest {
    private String businessId;
    private String regionId;
    private String stateId;
    private String outletId;
    private String orderTime;
    private List<Object> products;
}