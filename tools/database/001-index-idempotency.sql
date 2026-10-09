-- Run against bancadb with psql, outside a transaction.
-- Index the lookup executed by TransactionR2dbcRepository.findByIdempotencyKey.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_idempotency_key
    ON public.transactions USING btree (idempotency_key);
