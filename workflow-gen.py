#!/usr/bin/env python3
"""
This tool converts a user-defined job YAML file into an Argo Workflow YAML.

Each job must include:
  - name: a unique job name.
  - type: either "k8s" or "slurm".
  - jobSpec: (optional) a dictionary containing any valid Argo workflow template definition.
             For k8s jobs (or slurm jobs with a custom jobSpec), this defines the full template.
             If not provided for a slurm job, the task will use a templateRef.
  - inputs: (optional) a list of input definitions. Each input may contain:
        - name: the input name.
        - from: (optional) the source of the input.
                 If given as "jobName.outputName", then that output name is used.
                 If given as "jobName", then "result" is used as the default output name.
        - type: (optional) either "parameter" (default) or "artifact".
        - value: (optional) a literal value (for external inputs).

For slurm jobs without a custom jobSpec, the job must define:
  - command (e.g. the slurm submission command)
and the DAG task will use a templateRef to reference an externally defined slurm template.

The generated workflow will have:
  - A top-level DAG template ("example") that lists tasks.
  - Each task references either a separately defined template (for k8s and custom slurm)
    or uses a templateRef (for default slurm jobs).
  - Task arguments are built by merging any required job fields (like command)
    with input mappings. Input mappings from an upstream job will reference:
       - for parameters: "{{tasks.<source_job>.outputs.parameters.<output_name>}}"
       - for artifacts: "{{tasks.<source_job>.outputs.artifacts.<output_name>}}"
       where the default output_name is "result" if not explicitly provided.
  
Usage:
  python generate_workflow.py input.yaml output-workflow.yaml
"""

import argparse
import yaml
import sys

def load_yaml(file_path):
    """Load YAML file from the given path."""
    try:
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        sys.exit(f"Error loading YAML file: {e}")

def process_job_inputs(job):
    """
    Process the 'inputs' field of a job and return a dict with keys 'parameters' and 'artifacts'
    to be merged into the DAG task's arguments.
    
    Each input is expected to be a dict with:
      - name (str)
      - type (optional, defaults to "parameter")
      - either "from" (to indicate dependency) or "value" (for an external literal)
      
    If "from" is provided:
      - If it contains a dot (e.g. "job1.output1"), the part after the dot is used as the output name.
      - Otherwise, the default output name "result" is used.
    """
    args = {}
    params = []
    artifacts = []
    for inp in job.get("inputs", []):
        if not isinstance(inp, dict) or "name" not in inp:
            sys.exit("Each input must be a dict with at least a 'name' field.")
        input_name = inp["name"]
        input_type = inp.get("type", "parameter")
        if "from" in inp:
            source = inp["from"]
            if "." in source:
                source_job, output_name = source.split(".", 1)
            else:
                source_job = source
                output_name = "result"
            if input_type == "parameter":
                params.append({
                    "name": input_name,
                    "value": f"{{{{tasks.{source_job}.outputs.{output_name}}}}}"
                })
            elif input_type == "artifact":
                artifacts.append({
                    "name": input_name,
                    "from": f"{{{{tasks.{source_job}.outputs.artifacts.{output_name}}}}}"
                })
            else:
                sys.exit(f"Unsupported input type: {input_type}")
        elif "value" in inp:
            if input_type == "parameter":
                params.append({
                    "name": input_name,
                    "value": inp["value"]
                })
            elif input_type == "artifact":
                artifacts.append({
                    "name": input_name,
                    "from": inp["value"]
                })
            else:
                sys.exit(f"Unsupported input type: {input_type}")
        else:
            sys.exit(f"Input '{input_name}' must have either a 'from' or a 'value' field.")
    if params:
        args["parameters"] = params
    if artifacts:
        args["artifacts"] = artifacts
    return args

def merge_arguments(existing, new):
    """
    Merge two arguments dictionaries (each may contain 'parameters' and/or 'artifacts')
    in a way that if the same parameter name is present in both, the new value overrides.
    """
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
    """
    Build an Argo Workflow (as a Python dict) from a list of job definitions.
    
    For each job:
      - For k8s jobs (or slurm jobs with a custom jobSpec), a separate template is created using jobSpec.
      - For slurm jobs without a custom jobSpec, the task uses a templateRef.
      - Input definitions (both from dependencies and external) are added as arguments.
    """
    # Base workflow structure with a top-level DAG template named "example"
    workflow = {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Workflow",
        "metadata": {
            "generateName": "example-"
        },
        "spec": {
            "entrypoint": "example",
            "templates": [
                {
                    "name": "example",
                    "dag": {
                        "tasks": []
                    }
                }
            ]
        }
    }

    dag_tasks = []
    additional_templates = []
    used_templates = set()

    for job in jobs:
        if "name" not in job or "type" not in job:
            sys.exit("Every job must have a 'name' and a 'type' field.")
        task = {"name": job["name"]}
        
        # Process dependencies: if an input has a 'from', add that job as a dependency.
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
        
        # Process inputs to build task arguments.
        input_args = process_job_inputs(job)

        # Initialize task arguments.
        task_args = {}
        
        # For slurm jobs without a custom jobSpec, add command and s3artifact defaults.
        if job["type"] == "slurm" and "jobSpec" not in job:
            if "command" not in job:
                sys.exit(f"Slurm job '{job['name']}' requires a 'command' field when no jobSpec is provided.")
            cmd_param = {"name": "command", "value": job["command"]}
            task_args["parameters"] = [cmd_param]
            # Only add s3artifact default if not provided via an input mapping.
            existing_param_names = {p["name"] for p in task_args.get("parameters", [])}
            if "s3artifact" not in existing_param_names:
                s3_val = job.get("s3artifact", "NotSet")
                s3_param = {"name": "s3artifact", "value": s3_val}
                task_args["parameters"].append(s3_param)
        
        # Merge with additional input arguments.
        task_args = merge_arguments(task_args, input_args)
        if task_args:
            task["arguments"] = task_args

        # Determine how to reference the template.
        if job["type"] == "k8s":
            if "jobSpec" not in job:
                sys.exit(f"k8s job '{job['name']}' requires a jobSpec field.")
            tmpl_name = job.get("template", f"{job['name']}-template")
            task["template"] = tmpl_name
            if tmpl_name in used_templates:
                sys.exit(f"Duplicate template name detected: {tmpl_name}")
            used_templates.add(tmpl_name)
            tmpl_def = {"name": tmpl_name}
            tmpl_def.update(job["jobSpec"])
            additional_templates.append(tmpl_def)
        elif job["type"] == "slurm":
            if "jobSpec" in job:
                tmpl_name = job.get("template", f"{job['name']}-template")
                task["template"] = tmpl_name
                if tmpl_name in used_templates:
                    sys.exit(f"Duplicate template name detected: {tmpl_name}")
                used_templates.add(tmpl_name)
                tmpl_def = {"name": tmpl_name}
                tmpl_def.update(job["jobSpec"])
                additional_templates.append(tmpl_def)
            else:
                # Use templateRef to reference an externally defined slurm template.
                task["templateRef"] = {
                    "name": "slurm-template",
                    "template": "slurm-submit-job"
                }
        else:
            sys.exit(f"Unsupported job type: {job['type']}")

        dag_tasks.append(task)

    workflow["spec"]["templates"][0]["dag"]["tasks"] = dag_tasks
    workflow["spec"]["templates"].extend(additional_templates)
    return workflow

def main():
    parser = argparse.ArgumentParser(
        description="Generate an Argo Workflow YAML from a job description YAML file."
    )
    parser.add_argument("input_yaml", help="Path to the input YAML file containing job definitions")
    parser.add_argument("output_yaml", help="Path to the output Argo Workflow YAML file")
    args = parser.parse_args()

    data = load_yaml(args.input_yaml)
    if "jobs" not in data or not isinstance(data["jobs"], list):
        sys.exit("The input YAML must have a top-level 'jobs' key containing a list of jobs.")

    jobs = data["jobs"]
    workflow = build_workflow(jobs)

    try:
        with open(args.output_yaml, "w") as f:
            yaml.dump(workflow, f, default_flow_style=False)
        print(f"Argo Workflow YAML generated and saved to {args.output_yaml}")
    except Exception as e:
        sys.exit(f"Error writing output YAML file: {e}")

if __name__ == "__main__":
    main()
