WITH f AS ( --fees
        SELECT
                id,
                COALESCE(ROUND((fee::FLOAT/100)::NUMERIC,2),0) AS fee,
                COALESCE(ROUND((net::FLOAT/100)::NUMERIC,2),0) AS net,
                COALESCE(ROUND((amount::FLOAT/100)::NUMERIC,2),0) AS amount,
                exchange_rate
        FROM stripe.balance_transactions
        WHERE type='charge' -- select only charges, excl payouts, refunds and disputes
)

SELECT
        c.id,
        c.status,
        TO_TIMESTAMP(c.created) AS date_created,
        c.outcome::jsonb->>'type' AS charge_type,
        c.outcome::jsonb->>'reason' AS reason,
        c.outcome::jsonb->>'risk_level' AS risk_level,
        c.outcome::jsonb->>'network_status' AS network_status,
        c.outcome::jsonb->>'seller_message' AS seller_message,
        c.metadata::jsonb->>'order_id' AS order_id,
        c.failure_code,
        c.failure_message,
        c.payment_intent,
        (c.amount_refunded/100)::FLOAT AS refund_amount,
        (c.payment_method_details::jsonb->>'card')::jsonb->>'brand' AS card_brand,
        (c.payment_method_details::jsonb->>'card')::jsonb->>'last4' AS last4,
        ((c.payment_method_details::jsonb->>'card')::jsonb->>'checks')::jsonb->>'cvc_check' AS cvc_check,
        ((c.payment_method_details::jsonb->>'card')::jsonb->>'checks')::jsonb->>'address_line1_check' AS address_line1_check,
        ((c.payment_method_details::jsonb->>'card')::jsonb->>'checks')::jsonb->>'address_postal_code_check' AS address_postal_code_check,
        c.dispute,
        c.balance_transaction,
        b.type AS trx_type,
        b.exchange_rate,
        -- USD or source revenue currency
        c.currency AS currency_src,
        COALESCE(ROUND((c.amount::FLOAT/100)::NUMERIC,2),0) AS amount, -- original
        COALESCE(ROUND((f.fee/f.exchange_rate)::NUMERIC,2),0) AS fee_src,
        COALESCE(ROUND((f.net/f.exchange_rate)::NUMERIC,2),0) AS net_src,
        COALESCE(ROUND((f.amount/f.exchange_rate)::NUMERIC,2),0) AS amount_src,
        -- HKD payout currency
        b.currency AS currency_home,
        f.fee AS fee_home,
        f.net AS net_home,
        f.amount AS amount_home


FROM stripe.charges AS c
LEFT JOIN stripe.balance_transactions AS b ON c.balance_transaction=b.id
LEFT JOIN f ON f.id=b.id
