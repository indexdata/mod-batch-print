package org.folio.print.server.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class Message {
  private String notificationId;
  private String from;
  private String to;
  private String outputFormat;
  private String header;
  private String body;

}
