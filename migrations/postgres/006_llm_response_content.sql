ALTER TABLE traces ADD COLUMN IF NOT EXISTS llm_response_content JSONB;
