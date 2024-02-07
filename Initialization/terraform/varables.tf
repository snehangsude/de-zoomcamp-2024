# ---- Basic Handler ----

variable "credentials" {
  description = "Credentials JSON file"
  default = "~/.gc/credentials.json"
  type = string
}

variable "project" {
  description = "GCP Project ID"
  default = "dtc-de-course-0001"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Ref: https://cloud.google.com/about/locations"
  default = "asia-south1"
  type = string
}

# ---- Storage Handler ----

variable "storage_location" {
  description = "Location for GCP bucket"
  default     = "ASIA"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket"
  default = "STANDARD"
  type = string
}

variable "bucket_name" {
  description = "Name for G bucket"
  default     = "nyc_taxi_dataset"
  type = string
}

# ---- BigQuery Handler ----

variable "bigquery_dataset_name" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  default = "nyc_trips_data"
  type = string
}