terraform {
  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = "~> 2.0"
    }
  }
}

variable "do_token" {
  type      = string
  sensitive = true
}

variable "ssh_public_key_path" {
  type = string
}

provider "digitalocean" {
  token = var.do_token
}

locals {
  cloud_config = templatefile("${path.module}/cloud-config.yml", {
    public_key = file(var.ssh_public_key_path)
  })
}

resource "digitalocean_ssh_key" "deployer" {
  name       = "pg-batch-bench-vm-key"
  public_key = file(var.ssh_public_key_path)
}

resource "digitalocean_droplet" "pg-batch-bench-vm" {
  name          = "pg-batch-bench-vm"
  region        = "sgp1"
  size          = "s-8vcpu-16gb-amd"
  image         = "ubuntu-22-04-x64"
  ssh_keys      = [digitalocean_ssh_key.deployer.fingerprint]
  ipv6          = true
  droplet_agent = true
  tags          = ["terraform", "pg-batch-bench-vm"]

  user_data = local.cloud_config
}

output "public_ip" {
  value = digitalocean_droplet.pg-batch-bench-vm.ipv4_address
}
