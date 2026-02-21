terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.20.0"
    }
  }
}

provider "google" {
  # Configuration options
  project = "de-zoomcamp-terraform-486807"
  region  = "us-central1"
}

resource "google_storage_bucket" "data-lake-bucket" {
  name          = "khang-terraform-practice-gcp-1234"
  location      = "US"

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = "khang_dataset_terraform"
  project    = "de-zoomcamp-terraform-486807"
  location   = "US"
}