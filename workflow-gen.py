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
        - value: (optional) a literal value (for external inputs).

For slurm jobs (type "slurm"), the job must define:
  - command (e.g. the slurm submission command)
and the DAG task will use a templateRef to reference an externally defined slurm template.
If an input mapping contains a literal "value", it will be passed as the s3artifact parameter;
if only "from" is specified, then:
  - for non-slurm target jobs, the dependency is added and if the source is slurm the output artifact is used;
  - for slurm target jobs and the source is slurm, the input is passed as both:
      * a parameter "slurmInput" set to "true"
      * an artifact named "input-artifact" referencing the source’s output artifact.

Additionally, for slurm jobs if an outputs section is defined in the input (e.g. outputFileName and outputFilePath),
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

def process_job_inputs(job, job_type=None, job_types=None):
    """
    Process the 'inputs' field of a job and return a dict with keys 'parameters' and 'artifacts'
    to be merged into the DAG task's arguments.

    For non-slurm target jobs, each input must have a 'name' field.
    
    For slurm target jobs, the name is ignored:
      - If a literal "value" is provided, the parameter name is forced to "s3artifact".
      - If only "from" is provided and the source job is slurm, both:
            a parameter "slurmInput" with value "true" and
            an artifact with name "input-artifact" referencing "{{tasks.<source_job>.outputs.artifacts.output-artifact}}"
         are added.
      - Otherwise, nothing is added.

    For non-slurm target jobs, if "from" is provided:
      - If the referenced source job (looked up in job_types) is slurm, then regardless of any user‐specified type,
        an artifact argument is generated using the key "output-artifact":
            "{{tasks.<source_job>.outputs.artifacts.output-artifact}}"
      - Otherwise, if the input type is "artifact", use an artifact reference:
            "{{tasks.<source_job>.outputs.artifacts.<output_name>}}"
        else use a parameter reference:
            "{{tasks.<source_job>.outputs.parameters.<output_name>}}"
      - Here, <output_name> defaults to "result" if not explicitly provided.
    """
    args = {}
    params = []
    artifacts = []
    for inp in job.get("inputs", []):
        if job_type != "slurm":
            if not isinstance(inp, dict) or "name" not in inp:
                sys.exit("Each input must be a dict with at least a 'name' field for non-slurm jobs.")
        if "value" in inp:
            if job_type == "slurm":
                params.append({"name": "s3artifact", "value": inp["value"]})
            else:
                params.append({"name": inp["name"], "value": inp["value"]})
        elif "from" in inp:
            source = inp["from"]
            if "." in source:
                source_job, output_name = source.split(".", 1)
            else:
                source_job = source
                output_name = "result"
            if job_type != "slurm":
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
                if job_types and source_job in job_types and job_types[source_job] == "slurm":
                    # Add both the parameter and artifact for slurm target jobs.
                    params.append({"name": "slurmInput", "value": "true"})
                    artifacts.append({
                        "name": "input-artifact",
                        "from": f"{{{{tasks.{source_job}.outputs.artifacts.output-artifact}}}}"
                    })
                else:
                    continue
        else:
            sys.exit(f"Input '{inp.get('name', 'unknown')}' must have either a 'from' or a 'value' field.")
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
        task_args = merge_arguments(task_args, input_args)
        if task_args:
            task["arguments"] = task_args
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

def main():
    parser = argparse.ArgumentParser(description="Generate an Argo Workflow YAML from a job description YAML file.")
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
