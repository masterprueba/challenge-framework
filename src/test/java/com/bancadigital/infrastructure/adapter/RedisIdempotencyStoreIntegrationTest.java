package com.bancadigital.infrastructure.adapter;

import com.bancadigital.domain.exception.IdempotencyUnavailableException;
import com.bancadigital.domain.model.IdempotencyRecord;
import com.bancadigital.domain.model.Transaction;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "TRANSACTION_TEST_REDIS_PORT", matches = "[0-9]+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RedisIdempotencyStoreIntegrationTest {
    private final String prefix = "redis_regression:" + UUID.randomUUID() + ":";
    private LettuceConnectionFactory factory;
    private ReactiveStringRedisTemplate redis;
    private RedisIdempotencyStore store;

    @BeforeAll
    void setUp() {
        factory = new LettuceConnectionFactory("localhost", Integer.parseInt(System.getenv("TRANSACTION_TEST_REDIS_PORT")));
        factory.afterPropertiesSet();
        factory.start();
        redis = new ReactiveStringRedisTemplate(factory);
        store = new RedisIdempotencyStore(redis, JsonMapper.builder().findAndAddModules().build(),
                prefix, Duration.ofHours(24));
    }

    @AfterAll
    void tearDown() {
        if (redis != null) redis.scan(ScanOptions.scanOptions().match(prefix + "*").build())
                .flatMap(redis::delete).then().block(Duration.ofSeconds(10));
        if (factory != null) factory.destroy();
    }

    @Test
    void concurrentReservationsAcrossAdaptersHaveExactlyOneOwner() {
        RedisIdempotencyStore other = new RedisIdempotencyStore(redis,
                JsonMapper.builder().findAndAddModules().build(), prefix, Duration.ofHours(24));
        var results = Flux.range(0, 8).flatMap(index -> (index % 2 == 0 ? store : other)
                .reserve("concurrent", pending(UUID.randomUUID().toString()))).collectList()
                .block(Duration.ofSeconds(10));
        assertNotNull(results);
        assertEquals(1, results.stream().filter(Boolean.TRUE::equals).count());
        Duration remaining = redis.getExpire(prefix + "concurrent").block(Duration.ofSeconds(5));
        assertNotNull(remaining);
        assertTrue(remaining.compareTo(Duration.ofHours(23)) > 0);
        assertTrue(remaining.compareTo(Duration.ofHours(24)) <= 0);
    }

    @Test
    void completionRoundTripsTypedResultAndDoesNotExtendExpiration() {
        store.reserve("completed", pending("owner")).block(Duration.ofSeconds(5));
        // Shorten only this test key: completion must retain this TTL, not reset it to 24 hours.
        redis.expire(prefix + "completed", Duration.ofSeconds(30)).block(Duration.ofSeconds(5));
        Transaction transaction = Transaction.createPendingTransaction("OP", "WEB", new BigDecimal("100.00"),
                "10001", "20002", "WEB_OP").complete();
        store.complete("completed", new IdempotencyRecord("owner", "fingerprint", transaction, null, null))
                .block(Duration.ofSeconds(5));
        assertEquals(transaction, store.find("completed").block(Duration.ofSeconds(5)).transaction());
        Duration remaining = redis.getExpire(prefix + "completed").block(Duration.ofSeconds(5));
        assertNotNull(remaining);
        assertTrue(remaining.compareTo(Duration.ofSeconds(30)) <= 0);
        assertFalse(remaining.isNegative() || remaining.isZero());
    }

    @Test
    void expiredReservationAllowsNewOwnerAndOldOwnerCannotFinish() {
        RedisIdempotencyStore shortWindow = new RedisIdempotencyStore(redis,
                JsonMapper.builder().findAndAddModules().build(), prefix, Duration.ofMillis(200));
        assertEquals(true, shortWindow.reserve("expiration", pending("old")).block(Duration.ofSeconds(5)));
        Mono.delay(Duration.ofMillis(250)).block(Duration.ofSeconds(5));
        assertNull(store.find("expiration").block(Duration.ofSeconds(5)));
        assertEquals(true, store.reserve("expiration", pending("new")).block(Duration.ofSeconds(5)));
        StepVerifier.create(store.complete("expiration", new IdempotencyRecord("old", "fingerprint", null,
                        504, "timeout")))
                .expectError(IdempotencyUnavailableException.class).verify();
        assertEquals("new", store.find("expiration").block(Duration.ofSeconds(5)).owner());
    }
    private IdempotencyRecord pending(String owner) {
        return new IdempotencyRecord(owner, "fingerprint", null, null, null);
    }
}
