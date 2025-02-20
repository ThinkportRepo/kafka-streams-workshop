package com.thinkport.producer.model;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class ClickJson {
  private String clickId;
  private String userId;
  private String ip;
  private boolean knownIp;
  private String request;
  private int status;
  private int bytes;
  private String productId;
  private String referrer;
  private String userAgent;
}
