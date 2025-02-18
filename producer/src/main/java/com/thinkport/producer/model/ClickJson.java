package com.thinkport.producer.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
@AllArgsConstructor
@Builder
public class ClickJson {

    private String clickId;
    private String userId;
    private String ip;
    private boolean knownIp;
    //private String timestamp;
    private String request;
    private int status;
    private int bytes;
    private String productId;
    private String category;
    private BigDecimal price;
    private String referrer;
    //private String location;
    private String userAgent;


}
