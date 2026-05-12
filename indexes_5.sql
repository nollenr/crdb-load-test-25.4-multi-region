-- 5 secondary indexes per table

CREATE INDEX IF NOT EXISTS table_a_idx_01 ON table_a (tenant_code);
CREATE INDEX IF NOT EXISTS table_a_idx_02 ON table_a (account_ref);
CREATE INDEX IF NOT EXISTS table_a_idx_03 ON table_a (source_region, event_ts);
CREATE INDEX IF NOT EXISTS table_a_idx_04 ON table_a (customer_segment, effective_date);
CREATE INDEX IF NOT EXISTS table_a_idx_05 ON table_a (priority_code, is_active);

CREATE INDEX IF NOT EXISTS table_b_idx_01 ON table_b (merchant_id);
CREATE INDEX IF NOT EXISTS table_b_idx_02 ON table_b (channel_code, processing_date);
CREATE INDEX IF NOT EXISTS table_b_idx_03 ON table_b (approval_code);
CREATE INDEX IF NOT EXISTS table_b_idx_04 ON table_b (currency_code, submission_date);
CREATE INDEX IF NOT EXISTS table_b_idx_05 ON table_b (region_code, batch_number);

CREATE INDEX IF NOT EXISTS table_c_idx_01 ON table_c (status, created_at);
CREATE INDEX IF NOT EXISTS table_c_idx_02 ON table_c (workflow_name, updated_at);
CREATE INDEX IF NOT EXISTS table_c_idx_03 ON table_c (owner_team, status_date);
CREATE INDEX IF NOT EXISTS table_c_idx_04 ON table_c (model_version, risk_score);
CREATE INDEX IF NOT EXISTS table_c_idx_05 ON table_c (case_type, escalation_level);

CREATE INDEX IF NOT EXISTS table_d_idx_01 ON table_d (sku, shipment_date);
CREATE INDEX IF NOT EXISTS table_d_idx_02 ON table_d (category_code, warehouse_code);
CREATE INDEX IF NOT EXISTS table_d_idx_03 ON table_d (line_status, fulfillment_ts);
CREATE INDEX IF NOT EXISTS table_d_idx_04 ON table_d (tax_code, promised_date);
CREATE INDEX IF NOT EXISTS table_d_idx_05 ON table_d (is_backordered, shipment_date);
