package com.bancadigital.domain.model;

/** Keep reservations after errors or cancellation: transfers may have taken effect. */
public record IdempotencyRecord(String owner, String fingerprint, Transaction transaction,
                                Integer errorStatus, String errorMessage) {
    public boolean finished() { return transaction != null || errorStatus != null; }
}
