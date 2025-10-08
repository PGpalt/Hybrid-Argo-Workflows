#!/usr/bin/env python3
"""
This tool converts a user-defined job YAML file into an Argo Workflow YAML.

Each job must include:
  - name: a unique job name.
  - type: either "k8s" or "slurm".
  - jobSpec: (optional) a dictionary containing any valid Argo workflow template definition.
             For k8s jobs, this defines the full template.
             (For slurm jobs, jobSpec is not allowed.)
  - inputs: (optional) a list of input definitions. Each input may contain:
        - name: the input name (required for non-slurm jobs; ignored for slurm jobs)
        - from: (optional) the source of the input.
                 If given as "jobName.outputName", then that output name is used.
                 If given as "jobName", then "result" is used as the default output name.
        - type: (optional) either "parameter" (default) or "artifact".
        - value: (optional; **k8s only**) a literal value (for external inputs).
        - s3key: (optional; **slurm only**) a literal S3 key to pass to the slurm template.

For slurm jobs (type "slurm"), the job must define:
  - command (e.g. the slurm submission command)
and the DAG task will use a templateRef to reference an externally defined slurm template.

INPUT MAPPING (important behavioral points):
  • For non-slurm target jobs:
      - Each input must have a 'name'.
      - If the input comes from another job ("from"), then:
          - If the source job is slurm, an **artifact** arg is generated referencing the slurm job's "output-artifact".
          - Otherwise:
              - If input type is "artifact", reference the source artifact by name.
              - Else, use a parameter reference.
      - If the input is a literal, use "value" (unchanged behavior).

  • For slurm target jobs:
      - The 'name' field is ignored.
      - If an input uses a literal S3 key, specify it as **s3key** (not "value"). This is passed as parameter "s3artifact".
      - The compiler enforces that **s3key appears at most once per slurm job** (across all its inputs).
      - If an input only has "from" and the source job is slurm:
            * add parameter "slurmInput"="true"
            * add artifact named "input-artifact" from the source's "output-artifact"
        (Multiple such inputs are allowed; if multiple artifacts share the same name,
         the last one wins due to Argo argument name uniqueness.)
      - Using "value" on slurm jobs is not allowed (use "s3key" instead).

OUTPUTS:
  • For both k8s and slurm jobs, the job's **outputs** section continues to use **value**.
    (Only inputs for slurm jobs use **s3key**.)

Additionally, for slurm jobs if an outputs section is defined (e.g. outputFileName and outputFilePath),
those values are added as parameters so that the slurm template receives them.
In that case, if a slurm job defines outputs and is referenced by a non-slurm job, an extra parameter
fetchData is set to true.

The generated workflow will have:
  - A top-level DAG template ("hybrid-workflow") that lists tasks.
  - Each task references either a separately defined template (for k8s jobs)
    or uses a templateRef (for slurm jobs).
  - Task arguments are built by merging any required job fields with input mappings. Input mappings from an upstream job will reference:
       - for parameters: "{{tasks.<source_job>.outputs.parameters.<output_name>}}"
       - for artifacts: "{{tasks.<source_job>.outputs.artifacts.<output_name>}}"
       where the default output_name is "result" if not explicitly provided.
  
Usage:
  python3 workflow-gen.py input-job.yaml output-workflow.yaml
"""

import argparse
import yaml
import sys
import importlib.util


def load_yaml(file_path):
    """Load YAML file from the given path."""
    try:
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        sys.exit(f"Error loading YAML file: {e}")


def process_job_inputs(job, job_type=None, job_types=None):
    """
    Process the 'inputs' field of a job and return a dict with keys 'parameters' and 'artifacts'
    to be merged into the DAG task's arguments.

    For non-slurm target jobs, each input must have a 'name' field.

    For slurm target jobs:
      - 'name' is ignored.
      - If a literal **s3key** is provided, a parameter "s3artifact" is added with that value.
      - If the same input also includes **path**, a parameter "inputfilepath" is added with that value.
      - If only "from" is provided and the source job is slurm, add:
            parameters: {"slurmInput": "true"}
            artifacts:  {"input-artifact": "{{tasks.<source>.outputs.artifacts.output-artifact}}"}
      - Using "value" is not allowed (use "s3key" instead).
      - "path" is only valid when used together with "s3key".

    For non-slurm target jobs, if "from" is provided:
      - If the referenced source job is slurm, generate an artifact argument referencing "output-artifact".
      - Else, use artifact/parameter according to input type (default: parameter).
    """
    args = {}
    params = []
    artifacts = []

    s3key_used = False

    for inp in job.get("inputs", []):
        if job_type != "slurm":
            # Non-slurm inputs must be named
            if not isinstance(inp, dict) or "name" not in inp:
                sys.exit("Each input must be a dict with at least a 'name' field for non-slurm jobs.")

            # Non-slurm literal allowed via 'value' (unchanged)
            if "value" in inp:
                # 'path' is meaningless for k8s inputs; reject to avoid confusion
                if "path" in inp:
                    sys.exit(
                        f"Job '{job.get('name', '?')}' (type k8s) uses 'path' in inputs; "
                        "this is only valid for slurm inputs that use 's3key'."
                    )
                params.append({"name": inp["name"], "value": inp["value"]})
                continue

            # 's3key' is NOT valid for k8s inputs
            if "s3key" in inp:
                sys.exit(
                    f"Job '{job.get('name', '?')}' (type k8s) uses 's3key' in inputs; "
                    "this key is only valid for slurm jobs. Use 'value' instead for literals."
                )
            # Also block 'path' if it appears without s3key on k8s
            if "path" in inp:
                sys.exit(
                    f"Job '{job.get('name', '?')}' (type k8s) uses 'path' in inputs; "
                    "this is only valid for slurm inputs that use 's3key'."
                )

        # Slurm target job: enforce 's3key' semantics and reject 'value'
        if job_type == "slurm":
            if "value" in inp:
                sys.exit(
                    f"Job '{job.get('name', '?')}' (type slurm) uses a 'value' literal; "
                    "please use 's3key' instead."
                )

            # If 'path' is specified without 's3key', reject (it's only meaningful with s3key)
            if "path" in inp and "s3key" not in inp:
                sys.exit(
                    f"Job '{job.get('name', '?')}' (type slurm) specifies 'path' without 's3key'; "
                    "'path' is only valid when used together with 's3key'."
                )

            if "s3key" in inp:
                if s3key_used:
                    sys.exit(
                        f"Job '{job.get('name', '?')}' (type slurm) specifies 's3key' more than once; "
                        "only a single 's3key' literal is allowed per slurm job."
                    )
                s3key_used = True
                params.append({"name": "s3artifact", "value": inp["s3key"]})

                if "path" in inp:
                    params.append({"name": "inputFilePath", "value": inp["path"]})

                continue

        # Handle "from" references
        if "from" in inp:
            source = inp["from"]
            if "." in source:
                source_job, output_name = source.split(".", 1)
            else:
                source_job = source
                output_name = "result"

            if job_type != "slurm":
                # Non-slurm consumer of another job
                if job_types and source_job in job_types and job_types[source_job] == "slurm":
                    artifacts.append({
                        "name": inp["name"],
                        "from": f"{{{{tasks.{source_job}.outputs.artifacts.output-artifact}}}}"
                    })
                else:
                    if inp.get("type", "parameter") == "artifact":
                        artifacts.append({
                            "name": inp["name"],
                            "from": f"{{{{tasks.{source_job}.outputs.artifacts.{output_name}}}}}"
                        })
                    else:
                        params.append({
                            "name": inp["name"],
                            "value": f"{{{{tasks.{source_job}.outputs.parameters.{output_name}}}}}"
                        })
            else:
                # Slurm target job consuming from another slurm job
                if job_types and source_job in job_types and job_types[source_job] == "slurm":
                    params.append({"name": "slurmInput", "value": "true"})
                    artifacts.append({
                        "name": "input-artifact",
                        "from": f"{{{{tasks.{source_job}.outputs.artifacts.output-artifact}}}}"
                    })
                else:
                    # Non-slurm source to slurm target -> no direct mapping
                    continue
        else:
            # No 'value'/'s3key' and no 'from' -> invalid input spec
            # For slurm: also allow the case where we added s3key/path above (already continued)
            sys.exit(f"Input '{inp.get('name', 'unknown')}' must have either a 'from', 'value' (k8s), or 's3key' (slurm).")

    if params:
        args["parameters"] = params
    if artifacts:
        args["artifacts"] = artifacts
    return args


def merge_arguments(existing, new):
    merged = {}
    for key in ("parameters", "artifacts"):
        merged[key] = {}
        if key in existing:
            for item in existing[key]:
                merged[key][item["name"]] = item
        if key in new:
            for item in new[key]:
                merged[key][item["name"]] = item  # override any existing value
    result = {}
    for key in merged:
        if merged[key]:
            result[key] = list(merged[key].values())
    return result


def build_workflow(jobs):
    # Build a mapping of job name to job type.
    job_types = {job["name"]: job["type"] for job in jobs}
    # Precompute for each slurm job whether a non-slurm job uses its output.
    slurm_job_needs = {job["name"]: False for job in jobs if job["type"] == "slurm"}
    for job in jobs:
        if job["type"] != "slurm":
            for inp in job.get("inputs", []):
                if "from" in inp:
                    source = inp["from"]
                    if "." in source:
                        source_job, _ = source.split(".", 1)
                    else:
                        source_job = source
                    if source_job in slurm_job_needs:
                        slurm_job_needs[source_job] = True

    workflow = {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Workflow",
        "metadata": {"generateName": "hybrid-workflow-"},
        "spec": {
            "entrypoint": "hybrid-workflow",
            "templates": [{"name": "hybrid-workflow", "dag": {"tasks": []}}]
        }
    }

    dag_tasks = []
    additional_templates = []
    used_templates = set()

    for job in jobs:
        if "name" not in job or "type" not in job:
            sys.exit("Every job must have a 'name' and a 'type' field.")

        task = {"name": job["name"]}

        # Dependencies: any input that has 'from'
        dep_jobs = []
        for inp in job.get("inputs", []):
            if isinstance(inp, dict) and "from" in inp:
                source = inp["from"]
                if "." in source:
                    source_job, _ = source.split(".", 1)
                else:
                    source_job = source
                dep_jobs.append(source_job)
        if dep_jobs:
            task["dependencies"] = list(set(dep_jobs))

        # Build arguments from inputs
        input_args = process_job_inputs(job, job_type=job["type"], job_types=job_types)

        task_args = {}
        if job["type"] == "slurm":
            if "command" not in job:
                sys.exit(f"Slurm job '{job['name']}' requires a 'command' field.")
            cmd_param = {"name": "command", "value": job["command"]}
            task_args["parameters"] = [cmd_param]

            # If outputs are provided, add them. Also, if a non-slurm job uses this slurm job, force fetchData=true.
            if "outputs" in job:
                for output in job["outputs"]:
                    task_args["parameters"].append({"name": output["name"], "value": output["value"]})
                if slurm_job_needs.get(job["name"], False):
                    output_names = [output["name"] for output in job["outputs"]]
                    if "fetchData" not in output_names:
                        task_args["parameters"].append({"name": "fetchData", "value": "true"})

        # Merge input-derived args into any prebuilt arg set
        task_args = merge_arguments(task_args, input_args)
        if task_args:
            task["arguments"] = task_args

        # Template wiring
        if job["type"] == "k8s":
            if "jobSpec" not in job:
                sys.exit(f"k8s job '{job['name']}' requires a jobSpec field.")
            tmpl_name = job.get("template", f"{job['name']}-template")
            task["template"] = tmpl_name
            if tmpl_name in used_templates:
                sys.exit(f"Duplicate template name detected: {tmpl_name}")
            used_templates.add(tmpl_name)

            tmpl_def = {"name": tmpl_name}
            job_spec = job["jobSpec"]
            # Convenience: allow top-level image/command/args by wrapping into 'container'
            if "container" not in job_spec and any(k in job_spec for k in ["image", "command", "args"]):
                container_spec = {k: job_spec[k] for k in ["image", "command", "args"] if k in job_spec}
                new_job_spec = {"container": container_spec}
                for key, value in job_spec.items():
                    if key not in ["image", "command", "args"]:
                        new_job_spec[key] = value
                tmpl_def.update(new_job_spec)
            else:
                tmpl_def.update(job_spec)

            additional_templates.append(tmpl_def)

        elif job["type"] == "slurm":
            if "jobSpec" in job:
                sys.exit(f"Job '{job['name']}' of type slurm should not have a jobSpec.")
            task["templateRef"] = {"name": "slurm-template", "template": "slurm-submit-job"}

        else:
            sys.exit(f"Unsupported job type: {job['type']}")

        dag_tasks.append(task)

    workflow["spec"]["templates"][0]["dag"]["tasks"] = dag_tasks
    workflow["spec"]["templates"].extend(additional_templates)
    return workflow


def load_scheduler(scheduler_path):
    """Dynamically load the scheduler module from a given file path."""
    try:
        spec = importlib.util.spec_from_file_location("scheduler_module", scheduler_path)
        scheduler_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(scheduler_module)
        if not hasattr(scheduler_module, "apply_constraints"):
            sys.exit("The scheduler module must define an 'apply_constraints(workflow, jobs)' function.")
        return scheduler_module
    except Exception as e:
        sys.exit(f"Error loading scheduler module: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate an Argo Workflow YAML from a job description YAML file."
    )
    parser.add_argument("input_yaml", help="Path to the input YAML file containing job definitions")
    parser.add_argument("output_yaml", help="Path to the output Argo Workflow YAML file")
    parser.add_argument("--scheduler", help="Path to a custom scheduler Python file", default=None)
    args = parser.parse_args()

    data = load_yaml(args.input_yaml)
    if "jobs" not in data or not isinstance(data["jobs"], list):
        sys.exit("The input YAML must have a top-level 'jobs' key containing a list of jobs.")
    jobs = data["jobs"]

    workflow = build_workflow(jobs)

    # If a custom scheduler is provided, load it and apply its constraints.
    if args.scheduler:
        scheduler_module = load_scheduler(args.scheduler)
        workflow = scheduler_module.apply_constraints(workflow, jobs)

    try:
        with open(args.output_yaml, "w") as f:
            yaml.dump(workflow, f, default_flow_style=False)
        print(f"Argo Workflow YAML generated and saved to {args.output_yaml}")
    except Exception as e:
        sys.exit(f"Error writing output YAML file: {e}")


if __name__ == "__main__":
    main()
