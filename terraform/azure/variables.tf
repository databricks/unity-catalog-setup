variable "rg_name" {
  type = string
}

variable "location" {
  type = string
}

variable "reuse_rg" {
  type = bool
}

variable "tenant_id" {
  type = string
}

variable "subscription_id" {
  type = string
}

variable "prefix" {
  type = string
}

variable tags {
  type = map
}