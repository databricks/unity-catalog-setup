resource "random_string" "naming" {
  special = false
  upper   = false
  length  = 6
}

locals {
  suffix = random_string.naming.result
}