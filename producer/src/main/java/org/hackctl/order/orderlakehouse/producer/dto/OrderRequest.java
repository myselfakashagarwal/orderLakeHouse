package org.hackctl.order.orderlakehouse.producer.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.util.List;
import lombok.Data;

@Data
public class OrderRequest {

    // All meta

    @NotNull
    @Pattern(
        regexp = "^\\d{9}$",
        message = "businessId must be exactly 9 digits"
    )
    private String businessId;

    @NotNull
    @Pattern(regexp = "^\\d{3}$", message = "regionId must be exactly 3 digits")
    private String regionId;

    @NotNull
    @Pattern(regexp = "^\\d{6}$", message = "stateId must be exactly 6 digits")
    private String stateId;

    @NotNull
    @Pattern(
        regexp = "^\\d{11}$",
        message = "outletId must be exactly 11 digits"
    )
    private String outletId;

    @NotNull
    @Pattern(
        regexp = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}[+-]\\d{2}:\\d{2}$",
        message = "orderTime must be ISO-8601 with UTC offset e.g. 2026-05-29T10:30:00+05:30"
    )
    private String orderTime;

    @NotNull
    private List<Object> products;
}
