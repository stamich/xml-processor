package io.codeswarm.metric

case class SplunkEvent(
  workflow_id: String,
  business_date: String,
  ingestion_time: String,
  source_system: String,
  connector_type: String,
  reports_count: Int,
  start_time: String,
  end_time: String,
  duration_seconds: Long,
  timestamp: String
)
