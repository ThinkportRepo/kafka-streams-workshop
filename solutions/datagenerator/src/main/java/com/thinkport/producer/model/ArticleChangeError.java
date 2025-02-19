package com.thinkport.producer.model;

import com.example.avro.ChangeType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@Builder
public class ArticleChangeError {
  String articleId;
  ChangeType changeType;
  String reason;
}
