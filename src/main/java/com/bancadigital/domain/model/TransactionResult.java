package com.bancadigital.domain.model;

public record TransactionResult(Transaction transaction, boolean replayed) {}
