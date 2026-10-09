package com.bancadigital.infrastructure.adapter;

import com.bancadigital.domain.model.Transaction;
import com.bancadigital.domain.model.Transaction.TransactionStatus;
import com.bancadigital.domain.port.TransactionRepository;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Repository
public class TransactionR2dbcRepository implements TransactionRepository {

    private final R2dbcEntityTemplate entityTemplate;

    public TransactionR2dbcRepository(R2dbcEntityTemplate entityTemplate) {
        this.entityTemplate = entityTemplate;
    }

    @Override
    public Mono<Transaction> save(Transaction transaction) {
        return entityTemplate.getDatabaseClient()
                .sql("""
                        INSERT INTO transactions (transaction_id, operation_number, channel, amount,
                            status, created_at, updated_at, account_from, account_to, idempotency_key)
                        VALUES (:id, :operation, :channel, :amount, :status, :createdAt, :updatedAt,
                            :accountFrom, :accountTo, :idempotencyKey)
                        ON CONFLICT (transaction_id) DO UPDATE SET
                            operation_number = EXCLUDED.operation_number,
                            channel = EXCLUDED.channel,
                            amount = EXCLUDED.amount,
                            status = EXCLUDED.status,
                            updated_at = EXCLUDED.updated_at,
                            account_from = EXCLUDED.account_from,
                            account_to = EXCLUDED.account_to,
                            idempotency_key = EXCLUDED.idempotency_key
                        """)
                .bind("id", transaction.getTransactionId().toString())
                .bind("operation", transaction.getOperationNumber())
                .bind("channel", transaction.getChannel())
                .bind("amount", transaction.getAmount())
                .bind("status", transaction.getStatus().name())
                .bind("createdAt", transaction.getCreatedAt())
                .bind("updatedAt", transaction.getUpdatedAt())
                .bind("accountFrom", transaction.getAccountFrom())
                .bind("accountTo", transaction.getAccountTo())
                .bind("idempotencyKey", transaction.getIdempotencyKey())
                .fetch()
                .rowsUpdated()
                .thenReturn(transaction);
    }

    @Override
    public Mono<Transaction> findById(UUID transactionId) {
        Query query = Query.query(Criteria.where("transaction_id").is(transactionId.toString()));
        return entityTemplate.select(TransactionEntity.class)
                .matching(query)
                .first()
                .map(this::toDomain);
    }

    @Override
    public Mono<Transaction> findByIdempotencyKey(String idempotencyKey) {
        return entityTemplate.select(TransactionEntity.class)
                .from("transactions")
                .matching(Query.query(
                        Criteria.where("idempotency_key").is(idempotencyKey)))
                .first()
                .map(this::toDomain);
    }

    @Override
    public Mono<Boolean> existsByIdempotencyKey(String idempotencyKey) {
        return entityTemplate.getDatabaseClient()
                .sql("SELECT COUNT(*) FROM transactions WHERE idempotency_key = :key")
                .bind("key", idempotencyKey)
                .map((row, metadata) -> row.get(0, Long.class) > 0)
                .first();
    }

    @Override
    public Mono<Void> deleteById(UUID transactionId) {
        return entityTemplate.delete(TransactionEntity.class)
                .matching(Query.query(
                        Criteria.where("transaction_id").is(transactionId.toString())))
                .all()
                .then();
    }

    private Transaction toDomain(TransactionEntity entity) {
        return Transaction.builder()
                .transactionId(UUID.fromString(entity.getTransactionId()))
                .operationNumber(entity.getOperationNumber())
                .channel(entity.getChannel())
                .amount(entity.getAmount())
                .status(TransactionStatus.valueOf(entity.getStatus()))
                .createdAt(entity.getCreatedAt())
                .updatedAt(entity.getUpdatedAt())
                .accountFrom(entity.getAccountFrom())
                .accountTo(entity.getAccountTo())
                .idempotencyKey(entity.getIdempotencyKey())
                .build();
    }

    @Table("transactions")
    private static class TransactionEntity {
        @Id
        @Column("transaction_id")
        private String transactionId;

        @Column("operation_number")
        private String operationNumber;

        @Column("channel")
        private String channel;

        @Column("amount")
        private BigDecimal amount;

        @Column("status")
        private String status;

        @Column("created_at")
        private LocalDateTime createdAt;

        @Column("updated_at")
        private LocalDateTime updatedAt;

        @Column("account_from")
        private String accountFrom;

        @Column("account_to")
        private String accountTo;

        @Column("idempotency_key")
        private String idempotencyKey;

        public String getTransactionId() { return transactionId; }
        public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
        public String getOperationNumber() { return operationNumber; }
        public void setOperationNumber(String operationNumber) { this.operationNumber = operationNumber; }
        public String getChannel() { return channel; }
        public void setChannel(String channel) { this.channel = channel; }
        public BigDecimal getAmount() { return amount; }
        public void setAmount(BigDecimal amount) { this.amount = amount; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public LocalDateTime getCreatedAt() { return createdAt; }
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
        public LocalDateTime getUpdatedAt() { return updatedAt; }
        public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
        public String getAccountFrom() { return accountFrom; }
        public void setAccountFrom(String accountFrom) { this.accountFrom = accountFrom; }
        public String getAccountTo() { return accountTo; }
        public void setAccountTo(String accountTo) { this.accountTo = accountTo; }
        public String getIdempotencyKey() { return idempotencyKey; }
        public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    }
}
