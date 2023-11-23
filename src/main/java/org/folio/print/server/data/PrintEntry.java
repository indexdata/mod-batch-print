package org.folio.print.server.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.ZonedDateTime;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PrintEntry {

  private UUID id;

  private ZonedDateTime created;

  private PrintEntryType type;

  private String sortingField;

  private String content;
}
