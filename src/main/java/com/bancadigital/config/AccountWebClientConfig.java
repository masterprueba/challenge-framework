package com.bancadigital.config;

import io.netty.channel.ChannelOption;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

@Configuration
public class AccountWebClientConfig {

    @Bean(destroyMethod = "dispose")
    public ConnectionProvider accountSystemConnectionProvider(
            @Value("${account-system.pool.max-connections:200}") int maxConnections,
            @Value("${account-system.pool.pending-acquire-max-count:400}") int pendingMax,
            @Value("${account-system.pool.pending-acquire-timeout-ms:1000}") int acquireTimeoutMs,
            @Value("${account-system.pool.metrics-enabled:false}") boolean metricsEnabled) {
        return ConnectionProvider.builder("account-system")
                .maxConnections(maxConnections)
                .pendingAcquireMaxCount(pendingMax)
                .pendingAcquireTimeout(Duration.ofMillis(acquireTimeoutMs))
                .maxIdleTime(Duration.ofSeconds(30))
                .maxLifeTime(Duration.ofMinutes(5))
                .evictInBackground(Duration.ofSeconds(30))
                .metrics(metricsEnabled)
                .build();
    }

    @Bean
    public WebClient accountSystemHttpClient(
            WebClient.Builder builder,
            @Qualifier("accountSystemConnectionProvider") ConnectionProvider connections,
            @Value("${account-system.base-url:http://localhost:8081}") String baseUrl,
            @Value("${account-system.connection-timeout-ms:5000}") int connectionTimeoutMs,
            @Value("${account-system.response-timeout-ms:2000}") int responseTimeoutMs) {
        HttpClient client = HttpClient.create(connections)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeoutMs)
                .responseTimeout(Duration.ofMillis(responseTimeoutMs));
        return builder.clone()
                .baseUrl(baseUrl)
                .defaultHeader("Content-Type", "application/json")
                .defaultHeader("Accept", "application/json")
                .clientConnector(new ReactorClientHttpConnector(client))
                .build();
    }
}
