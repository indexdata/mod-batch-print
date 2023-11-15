package org.folio.print.server.storage;

public class ForbiddenException extends RuntimeException {
  public ForbiddenException() {
    super("Forbidden");
  }

}
