package com.bancadigital.domain.port;

import com.bancadigital.domain.model.Transaction;
import reactor.core.publisher.Mono;
import java.util.UUID;

public interface TransactionRepository {
    Mono<Transaction> save(Transaction transaction);
    Mono<Transaction> findById(UUID transactionId);
}
