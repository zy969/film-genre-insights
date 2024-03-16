

provider "google" {
  credentials = "credentials.json"

  project = "thermal-formula-416221"
  region  = "us-central1"
}


resource "google_storage_bucket" "static" {
  name          = "movie-zy969"
  location      = "us-central1"
  uniform_bucket_level_access = true
  storage_class = "STANDARD"
}


resource "google_bigquery_dataset" "dataset" {
  dataset_id = "movie"
  project    = "thermal-formula-416221"
  location   = "US"
}
