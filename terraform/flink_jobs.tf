locals {
  project_dir = abspath("${path.module}/..")
  flink_job_files = fileset("${local.project_dir}/flink/sql/jobs", "*.sql")

}

resource "terraform_data" "flink_job" {
  for_each = local.flink_job_files

  triggers_replace = [
    filemd5("${local.project_dir}/flink/sql/jobs/${each.value}"),
    # Redeploy when the stack changes, since containers may have been
    # recreated without their jobs.
    docker_compose.app.content_hash,
  ]

  input = {
    project_dir = local.project_dir
    sql_file    = "flink/sql/jobs/${each.value}"
    python      = var.python_command
  }

  provisioner "local-exec" {
    working_dir = self.input.project_dir
    command     = "${self.input.python} flink/deploy_job.py ${self.input.sql_file}"
  }

  # Runs before the stack is removed, because this resource depends on it.
  provisioner "local-exec" {
    when        = destroy
    working_dir = self.input.project_dir
    command     = "${self.input.python} flink/deploy_job.py --cancel ${self.input.sql_file}"
    on_failure  = continue
  }
}

output "flink_jobs" {
  description = "Flink SQL jobs deployed to the local stack."
  value       = sort(keys(terraform_data.flink_job))
}
