package com.thinkport.producer.model;

import lombok.*;

@AllArgsConstructor
@Getter
@Setter
@Builder
@NoArgsConstructor
@ToString
public class User {
    private String name;
    private String dept;
    private String salary;
}
