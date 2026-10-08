package com.bancadigital.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class IdempotencyConfig {

    @Value("${app.idempotency.ttl-hours:24}")
    private int ttlHours;

    @Value("${app.idempotency.key-prefix:idempotency:}")
    private String keyPrefix;

    private final ReactiveRedisConnectionFactory connectionFactory;

    @Bean
    public ReactiveStringRedisTemplate reactiveStringRedisTemplate() {
        return new ReactiveStringRedisTemplate(connectionFactory);
    }

    @Bean
    public ReactiveRedisTemplate<String, String> reactiveRedisTemplate(
            ReactiveRedisConnectionFactory factory) {

        StringRedisSerializer serializer = new StringRedisSerializer();
        RedisSerializationContext<String, String> context = RedisSerializationContext
                .<String, String>newSerializationContext(serializer)
                .key(serializer)
                .value(serializer)
                .hashKey(serializer)
                .hashValue(serializer)
                .build();

        return new ReactiveRedisTemplate<>(factory, context);
    }

    @Bean
    public IdempotencyService idempotencyService(
            ReactiveStringRedisTemplate redisTemplate) {

        return new IdempotencyService(redisTemplate, keyPrefix, Duration.ofHours(ttlHours));
    }

    @Slf4j
    public static class IdempotencyService {

        private final ReactiveStringRedisTemplate redisTemplate;
        private final String keyPrefix;
        private final Duration ttl;

        public IdempotencyService(ReactiveStringRedisTemplate redisTemplate,
                                  String keyPrefix,
                                  Duration ttl) {
            this.redisTemplate = redisTemplate;
            this.keyPrefix = keyPrefix;
            this.ttl = ttl;
        }

        public <T> Mono<T> execute(String idempotencyKey,
                                    Function<String, Mono<T>> action,
                                    Function<T, String> serializer) {
            String fullKey = keyPrefix + idempotencyKey;

            return redisTemplate.hasKey(fullKey)
                    .flatMap(exists -> {
                        if (Boolean.TRUE.equals(exists)) {
                            log.info("Clave de idempotencia encontrada en caché: {}", fullKey);
                            return redisTemplate.opsForValue().get(fullKey)
                                    .flatMap(value -> {
                                        T cached = deserialize(value, serializer);
                                        if (cached != null) {
                                            return Mono.just(cached);
                                        }
                                        return action.apply(idempotencyKey)
                                                .flatMap(result -> cacheResult(fullKey, result, serializer));
                                    });
                        }
                        return action.apply(idempotencyKey)
                                .flatMap(result -> cacheResult(fullKey, result, serializer));
                    });
        }

        private <T> Mono<T> cacheResult(String key, T result, Function<T, String> serializer) {
            String serialized = serializer.apply(result);
            return redisTemplate.opsForValue()
                    .set(key, serialized, ttl)
                    .then(Mono.just(result))
                    .doOnNext(r -> log.info("Resultado cacheado con clave de idempotencia: {}", key));
        }

        @SuppressWarnings("unchecked")
        private <T> T deserialize(String value, Function<T, String> serializer) {
            try {
                return (T) value;
            } catch (Exception e) {
                log.warn("Error al deserializar valor cacheado: {}", e.getMessage());
                return null;
            }
        }

        public Mono<Boolean> exists(String idempotencyKey) {
            String fullKey = keyPrefix + idempotencyKey;
            return redisTemplate.hasKey(fullKey);
        }

        public Mono<Void> delete(String idempotencyKey) {
            String fullKey = keyPrefix + idempotencyKey;
            return redisTemplate.delete(fullKey).then();
        }
    }
}
