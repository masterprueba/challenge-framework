package com.bancadigital.domain.port;

import com.bancadigital.domain.model.IdempotencyRecord;
import reactor.core.publisher.Mono;

public interface IdempotencyStore {
    /** Atomically reserve the key and its expiration. */
    Mono<Boolean> reserve(String key, IdempotencyRecord pending);
    Mono<IdempotencyRecord> find(String key);
    /** Only the owner can finish; preserve the original expiration. */
    Mono<Void> complete(String key, IdempotencyRecord result);
}
