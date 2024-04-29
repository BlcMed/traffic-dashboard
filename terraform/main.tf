terraform {
    required_providers {
        google = {
        source  = "hashicorp/google"
        version = "3.5.0"
        }
    }
}

provider "google" {
    project     = var.project_id
    region      = var.region
}

# Load variables from JSON file
locals {
  config = jsondecode(file("config/credentials.json"))
}

# Define Google Cloud Composer environment
resource "google_composer_environment" "my_composer"{
    name = var.composer_env_name
    region  = var.composer_location
    config {
        node_count = 3
        software_config {
            image_version = var.composer_version

            pypi_packages = {
              google-api-core="==2.17.1"
              google-auth="==2.28.2"
              google-cloud-core="==2.4.1"
              google-cloud-storage="==2.15.0"
              google-crc32c="==1.5.0"
              google-resumable-media="==2.7.0"
              googleapis-common-protos="==1.63.0"
              pandas="==2.2.1"
              requests="==2.31.0"
            }

            env_variables = {
              base_url = config.base_url
              key = config.key
              bucket_name = config.bucket_name
            }

        }
    }
}

