package org.folio.print.server.storage;

public class NotFoundException extends RuntimeException {
  public NotFoundException() {
    super("Not Found");
  }
}
