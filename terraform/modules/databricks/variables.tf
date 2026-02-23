variable "catalog_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "project_name" {
  type = string
}

variable "storage_credential_name" {
  type = string
}

variable "external_location_url" {
  type = string
}

variable "landing_bucket_arn" {
  type = string
}

variable "roles" {
  description = "Map of role name to list of principals"
  type        = map(list(string))
  default     = {}
}
