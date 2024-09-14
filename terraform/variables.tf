
locals {
  data_lake_bucket = "data_lake_card"
}

variable "project" {
  description = "Your GCP project ID"
  default     = "dataengineering1-433721"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "us-west1"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "bq_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "mtg_card_data_raw"
}

variable "dbt_dataset" {
  description = "DBT dataset that will feed into the visualization layer"
  type        = string
  default     = "mtg_card_data_dbt"
}
