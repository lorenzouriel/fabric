terraform {
  required_version = ">= 1.8, < 2.0"

  # Organization comes from TF_CLOUD_ORGANIZATION. The HCP workspace must use
  # "Local" execution mode while authentication relies on `az login`.
  cloud {
    workspaces {
      name = "senado360-dev"
    }
  }

  required_providers {
    fabric = {
      source  = "microsoft/fabric"
      version = "~> 1.14"
    }
  }
}

provider "fabric" {
  use_cli = true
}
