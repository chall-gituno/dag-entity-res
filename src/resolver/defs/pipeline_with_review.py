import dagster as dg
from resolver.defs.jobs import er_pipeline_job
from resolver.defs.assets.resolve_companies import validate_resolved_job


@dg.op(description=
       "Run the full ER pipeline (blocking, pairing, features, scoring, entities).")
def run_er_pipeline(context) -> bool:
  context.log.info("▶ Running main ER pipeline job...")
  result = er_pipeline_job.execute_in_process()
  if not result.success:
    raise Exception("Main ER pipeline failed.")
  context.log.info("✅ Main pipeline completed successfully.")
  return True


@dg.op(description="Run final system checks and AI validation after main pipeline.")
def run_final_review(context, _: bool):
  context.log.info("▶ Running final review job...")
  result = validate_resolved_job.execute_in_process()
  if result.success:
    context.log.info("✅ Final review completed successfully.")
  else:
    context.log.warning("⚠️ Final review failed — check logs for details.")


@dg.job(
  name="er_pipeline_with_review",
  description=(
    "Runs the entire Entity Resolution pipeline (blocking → pairing → features → scoring → "
    "entities) and then performs the final review checks."),
)
def er_pipeline_with_review():
  run_final_review(run_er_pipeline())
