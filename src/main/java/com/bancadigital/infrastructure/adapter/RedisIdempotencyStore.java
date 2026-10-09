package com.bancadigital.infrastructure.adapter;

import com.bancadigital.domain.exception.IdempotencyUnavailableException;
import com.bancadigital.domain.model.IdempotencyRecord;
import com.bancadigital.domain.port.IdempotencyStore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

public class RedisIdempotencyStore implements IdempotencyStore {
    private static final RedisScript<Long> COMPLETE = RedisScript.of("""
            local value = redis.call('GET', KEYS[1])
            if not value then return 0 end
            local entry = cjson.decode(value)
            if entry.owner ~= ARGV[1] then return 0 end
            local ttl = redis.call('PTTL', KEYS[1])
            if ttl <= 0 then return 0 end
            redis.call('SET', KEYS[1], ARGV[2], 'PX', ttl)
            return 1
            """, Long.class);
    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final String prefix;
    private final Duration ttl;

    public RedisIdempotencyStore(ReactiveStringRedisTemplate redis, ObjectMapper mapper,
                                 String prefix, Duration ttl) {
        if (ttl.isZero() || ttl.isNegative()) throw new IllegalArgumentException("TTL debe ser positivo");
        this.redis = redis;
        this.mapper = mapper;
        this.prefix = prefix;
        this.ttl = ttl;
    }

    @Override
    public Mono<Boolean> reserve(String key, IdempotencyRecord pending) {
        return Mono.defer(() -> redis.opsForValue().setIfAbsent(prefix + key, encode(pending), ttl))
                .onErrorMap(this::unavailable);
    }

    @Override
    public Mono<IdempotencyRecord> find(String key) {
        return redis.opsForValue().get(prefix + key)
                .map(value -> {
                    try { return mapper.readValue(value, IdempotencyRecord.class); }
                    catch (JsonProcessingException error) { throw unavailable(error); }
                }).onErrorMap(this::unavailable);
    }

    @Override
    public Mono<Void> complete(String key, IdempotencyRecord result) {
        return Mono.defer(() -> redis.execute(COMPLETE, List.of(prefix + key),
                        List.of(result.owner(), encode(result))).single())
                .flatMap(updated -> updated == 1 ? Mono.<Void>empty() : Mono.error(
                        new IdempotencyUnavailableException("La reserva de idempotencia ya no está disponible", null)))
                .onErrorMap(this::unavailable);
    }

    private String encode(IdempotencyRecord value) {
        try { return mapper.writeValueAsString(value); }
        catch (JsonProcessingException error) { throw unavailable(error); }
    }

    private IdempotencyUnavailableException unavailable(Throwable error) {
        return error instanceof IdempotencyUnavailableException existing ? existing :
                new IdempotencyUnavailableException("Servicio de idempotencia Redis no disponible", error);
    }
}
