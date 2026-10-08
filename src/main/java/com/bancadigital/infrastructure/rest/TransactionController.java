package com.bancadigital.infrastructure.rest;

import com.bancadigital.application.usecase.ProcessTransactionUseCase;
import com.bancadigital.domain.model.Transaction;
import com.bancadigital.domain.model.Transaction.TransactionStatus;
import com.bancadigital.domain.port.TransactionRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/transactions")
@RequiredArgsConstructor
@Validated
@Tag(name = "Transacciones", description = "API para procesamiento de transacciones bancarias")
@Slf4j
public class TransactionController {

    private final ProcessTransactionUseCase processTransactionUseCase;
    private final TransactionRepository transactionRepository;

    @PostMapping
    @Operation(summary = "Procesar transacción", description = "Procesa una nueva transacción bancaria con soporte de idempotencia")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "201", description = "Transacción procesada exitosamente",
                     content = @Content(schema = @Schema(implementation = TransactionResponse.class))),
        @ApiResponse(responseCode = "200", description = "Transacción duplicada - retorna la transacción original",
                     content = @Content(schema = @Schema(implementation = TransactionResponse.class))),
        @ApiResponse(responseCode = "400", description = "Datos de entrada inválidos"),
        @ApiResponse(responseCode = "422", description = "Error de negocio - cuenta inválida o fondos insuficientes"),
        @ApiResponse(responseCode = "503", description = "Sistema de cuentas no disponible"),
        @ApiResponse(responseCode = "504", description = "Timeout del sistema de cuentas")
    })
    public Mono<ResponseEntity<TransactionResponse>> processTransaction(
            @Valid @RequestBody TransactionRequest request) {

        log.info("Recibida solicitud de transacción: operationNumber={}, channel={}, amount={}",
                request.operationNumber(), request.channel(), request.amount());

        String idempotencyKey = buildIdempotencyKey(request.operationNumber(), request.channel());

        return transactionRepository.findByIdempotencyKey(idempotencyKey)
                .flatMap(existing -> {
                    log.info("Transacción idempotente encontrada: {}", existing.getTransactionId());
                    return Mono.just(ResponseEntity.ok(toResponse(existing)));
                })
                .switchIfEmpty(
                    processTransactionUseCase.execute(
                            request.operationNumber(),
                            request.channel(),
                            request.accountFrom(),
                            request.accountTo(),
                            request.amount()
                    )
                    .map(transaction -> {
                        HttpStatus status = transaction.getStatus() == TransactionStatus.COMPLETED
                                ? HttpStatus.CREATED
                                : HttpStatus.UNPROCESSABLE_ENTITY;
                        return ResponseEntity.status(status).body(toResponse(transaction));
                    })
                )
                .onErrorResume(ResponseStatusException.class, e ->
                    Mono.just(ResponseEntity.status(e.getStatusCode())
                            .body(TransactionResponse.error(e.getReason())))
                )
                .onErrorResume(Exception.class, e -> {
                    log.error("Error inesperado procesando transacción", e);
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body(TransactionResponse.error("Error interno del servidor")));
                });
    }

    @GetMapping("/{transactionId}")
    @Operation(summary = "Consultar transacción", description = "Obtiene los detalles de una transacción por su ID")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Transacción encontrada",
                     content = @Content(schema = @Schema(implementation = TransactionResponse.class))),
        @ApiResponse(responseCode = "404", description = "Transacción no encontrada")
    })
    public Mono<ResponseEntity<TransactionResponse>> getTransaction(
            @Parameter(description = "ID de la transacción") @PathVariable UUID transactionId) {

        return transactionRepository.findById(transactionId)
                .map(transaction -> ResponseEntity.ok(toResponse(transaction)))
                .switchIfEmpty(Mono.just(ResponseEntity.notFound().build()));
    }

    @GetMapping("/idempotency/{idempotencyKey}")
    @Operation(summary = "Verificar clave de idempotencia", description = "Verifica si existe una transacción con la clave de idempotencia dada")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Consulta exitosa"),
        @ApiResponse(responseCode = "404", description = "No existe transacción con esa clave")
    })
    public Mono<ResponseEntity<Map<String, Object>>> checkIdempotency(
            @Parameter(description = "Clave de idempotencia") @PathVariable String idempotencyKey) {

        return transactionRepository.findByIdempotencyKey(idempotencyKey)
                .map(transaction -> ResponseEntity.ok(Map.<String, Object>of(
                        "exists", true,
                        "transactionId", transaction.getTransactionId(),
                        "status", transaction.getStatus(),
                        "createdAt", transaction.getCreatedAt()
                )))
                .switchIfEmpty(Mono.just(ResponseEntity.ok(Map.<String, Object>of("exists", false))));
    }

    private String buildIdempotencyKey(String operationNumber, String channel) {
        return operationNumber + "_" + channel;
    }

    private TransactionResponse toResponse(Transaction transaction) {
        return new TransactionResponse(
                transaction.getTransactionId(),
                transaction.getOperationNumber(),
                transaction.getChannel(),
                transaction.getAmount(),
                transaction.getStatus().name(),
                transaction.getCreatedAt(),
                transaction.getUpdatedAt(),
                transaction.getAccountFrom(),
                transaction.getAccountTo()
        );
    }

    public record TransactionRequest(
            @NotBlank(message = "El número de operación es obligatorio")
            String operationNumber,
            @NotBlank(message = "El canal es obligatorio")
            String channel,
            @NotNull(message = "El monto es obligatorio")
            @Positive(message = "El monto debe ser positivo")
            BigDecimal amount,
            @NotBlank(message = "La cuenta de origen es obligatoria")
            String accountFrom,
            @NotBlank(message = "La cuenta de destino es obligatoria")
            String accountTo
    ) {}

    public record TransactionResponse(
            UUID transactionId,
            String operationNumber,
            String channel,
            BigDecimal amount,
            String status,
            LocalDateTime createdAt,
            LocalDateTime updatedAt,
            String accountFrom,
            String accountTo
    ) {
        public static TransactionResponse error(String message) {
            return new TransactionResponse(
                    null, null, null, null, "ERROR",
                    null, null, null, null
            );
        }
    }
}
