package com.bancadigital;






import com.bancadigital.application.usecase.ProcessTransactionUseCase;
import com.bancadigital.config.IdempotencyConfig;
import com.bancadigital.config.ResilienceConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.reactive.config.EnableWebFlux;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

@SpringBootApplication
@EnableR2dbcRepositories
@EnableCaching
@EnableWebFlux
@EnableAsync
@ConfigurationPropertiesScan
@Import({ResilienceConfig.class, IdempotencyConfig.class})
public class Application {

    private final ProcessTransactionUseCase processTransactionUseCase;
    private final reactor.core.scheduler.Scheduler boundedElasticScheduler;

    public Application(ProcessTransactionUseCase processTransactionUseCase) {
        this.processTransactionUseCase = processTransactionUseCase;
        this.boundedElasticScheduler = Schedulers.newBoundedElastic(10, 100, "transaction-scheduler");
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public CircuitBreakerConfig defaultCircuitBreakerConfig() {
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofMillis(1000))
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(5)
                .recordExceptions(TimeoutException.class, java.util.concurrent.TimeoutException.class)
                .build();
    }

    @Bean
    public RetryConfig defaultRetryConfig() {
        return RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(TimeoutException.class, java.util.concurrent.TimeoutException.class)
                .build();
    }

    @Bean
    public TimeLimiterConfig defaultTimeLimiterConfig() {
        return TimeLimiterConfig.custom()
                .timeoutDuration(Duration.ofSeconds(2))
                .build();
    }

    public Mono<Void> warmup() {
        return Mono.fromRunnable(() -> {
                    // Simulación de carga inicial para verificar configuración
                    processTransactionUseCase.execute(
                            "OP123456",
                            "MOBILE",
                            "10001",
                            "20002",
                            new BigDecimal("100.0")
                    ).subscribeOn(boundedElasticScheduler)
                    .subscribe(
                            result -> System.out.println("Warmup transaction processed: " + result),
                            error -> System.err.println("Warmup error: " + error.getMessage())
                    );
                })
                .then();
    }
}
