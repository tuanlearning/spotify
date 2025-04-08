terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "spotify_bucket" {
  name     = "spotify_tuanlg"
  location = var.region
  force_destroy = true
}

resource "google_bigquery_dataset" "spotify_stg" {
  dataset_id = "spotify_stg"
  location   = var.region
}

resource "google_bigquery_dataset" "spotify" {
  dataset_id = "spotify"
  location   = var.region
}