terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.11.0"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
  credentials = file(var.credentials) 
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "nyc_taxi_dataset" {
  name          = var.bucket_name
  location      = var.region
  storage_class = var.storage_class
  uniform_bucket_level_access = true
  versioning {
    enabled     = true
  }
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 
    }
  }
  force_destroy = true
}

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "nyc_trips_data" {
  dataset_id = var.bigquery_dataset_name
  project    = var.project
  location   = var.region
}