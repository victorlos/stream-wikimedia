locals {
  project_dir = abspath("${path.module}/..")
}

# docker_compose starts containers but never builds images, so the custom
# Flink image (flink/Dockerfile) is built here first, and rebuilt whenever
# the Dockerfile changes.
resource "terraform_data" "flink_image" {
  triggers_replace = [filemd5("${local.project_dir}/flink/Dockerfile")]

  provisioner "local-exec" {
    working_dir = local.project_dir
    command     = "docker compose build jobmanager"
  }
}