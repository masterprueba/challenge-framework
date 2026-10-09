package com.bancadigital.config;

import com.bancadigital.domain.port.IdempotencyStore;
import com.bancadigital.infrastructure.adapter.RedisIdempotencyStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

import java.time.Duration;

@Configuration
public class IdempotencyConfig {
    @Bean
    public IdempotencyStore idempotencyStore(ReactiveStringRedisTemplate redis, ObjectMapper mapper,
            @Value("${idempotency.key-prefix:idempotency:}") String prefix,
            @Value("${idempotency.ttl-hours:24}") int ttlHours) {
        return new RedisIdempotencyStore(redis, mapper, prefix, Duration.ofHours(ttlHours));
    }
}
