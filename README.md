Installation and Usage Guide!

Step by Step:
1. A working Kubernetes Cluster with Argo Workflows installed (https://argo-workflows.readthedocs.io/en/latest/quick-start/).
2. Configure Argo Workflows Artifact Repository Ref in the argo namespace (https://argo-workflows.readthedocs.io/en/latest/artifact-repository-ref/).
3. A Slurm Cluster where you have SSH Access and the required modules are available (gnu/14 , python/3.8 , pytorch/2.9.0 , anaconda/2024.10) or compatible versions.
4. If there is no access to a Slurm Cluster build the dockerfile inside the Slurm folder locally with:
   1. build -t slurm-container:latest .
   2. docker run -it -h slurmctl -p 2220:22 --cap-add sys_admin slurm-container:latest
   3. Once inside the container run /usr/sbin/sshd to launch the ssh Client
   4. minikube addons enable ingress
   5. Apply the ssh-creds-example.yaml with host:host.minikube.internal user:root and port:"2220"
   6. generate an SSH key pair and add the public key to /root/.ssh/authorized_keys inside the Slurm Container
6. To run the Katib Experiment example you need to install Katib Kubeflows configured for Argo Workflows as described here (https://github.com/kubeflow/katib/blob/master/examples/v1beta1/argo/README.md)
7. Create and apply the two Kubernetes Secrets containing your SSH Credentials as in the examples (slurm-ssh-key-example.yaml / ssh-creds-example.yaml)
8. Apply the Role Based Access Authorization rules for the Cluster Workflow Template (workflow-RBAC.yaml) ClusterRole and ClusterRoleBinding.
9. Apply the slurm-job-workflow-template.yaml (Argo ClusterWorkflowTemplate for Slurm job Submission)

Execution: 
1. python3 workflow-gen.py input-hybrid-workflow.yaml output-argo-workflow.yaml --scheduler=my_scheduler.py (scheduler flag is optional and names are indicative)
2. Argo submit output-argo-workflow.yaml

Hybrid Workflow Schema:

This tool converts a user-defined job YAML file into an Argo Workflow YAML.

Each job must include:
   - name: a unique job name.
   - type: either "k8s" or "slurm".
   - jobSpec: (optional) a dictionary containing any valid Argo workflow template definition (container,script) (https://argo-workflows.readthedocs.io/en/latest/workflow-concepts/#container).
             For k8s jobs, this defines the full template.
             (For slurm jobs, jobSpec is not allowed.)
   - inputs: (optional) a list of input definitions. Each input may contain:
      - name: the input name (required for k8s jobs; ignored for slurm jobs) must match the input parameter/artifact name inside the jobSpec: definition
      - from: (optional) the source of the input.
               If given as "jobName.outputName", then that output name is used.
               If given as "jobName", then "result" is used as the default output name.
      - type: (optional) either "parameter" (default) or "artifact".
      - s3key: (optional; **slurm only**) a literal S3 key to pass to the slurm template.
      - path: (optional; **slurm only**) the path on Slurm where the S3 Artifact will be uploaded
      - cleanDataPath:(optional; **slurm only**) the path where the clean-up will be performed

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
   • For slurm target jobs:
      - The 'name' field is ignored.
      - If an input uses a literal S3 key, specify it as **s3key**. This is passed as parameter "s3artifact".
      - The compiler enforces that **s3key appears at most once per slurm job** (across all its inputs).
      - If an input only has "from" and the source job is slurm:
            * add parameter "slurmInput"="true"
            * add artifact named "input-artifact" from the source's "output-artifact"
           (Multiple such inputs are allowed; if multiple artifacts share the same name,
            the last one wins due to Argo argument name uniqueness.)


Additionally, for slurm jobs if an outputs section is defined (e.g. outputFileName, outputFilePath,
or cleanDataPath), those values are added as parameters so that the slurm template receives them.
In that case, if a slurm job defines outputs and is referenced by a non-slurm job, an extra parameter
fetchData is set to true. Slurm tasks with no downstream slurm dependents get cleanData=true **only when**
cleanDataPath is set, and slurm tasks with downstream slurm dependents get a cleanup task that depends
on the upstream slurm task and its immediate downstream slurm tasks and runs
`rm -rf -- "<cleanDataPath>"` (cleanup is skipped entirely if cleanDataPath is not set). This is
computed before any custom scheduler adjustments are applied.

The generated workflow will have:
  - A top-level DAG template ("hybrid-workflow") that lists tasks.
  - Each task references either a separately defined template (for k8s jobs)
    or uses a templateRef (for slurm jobs).
  - Task arguments are built by merging any required job fields with input mappings. Input mappings from an upstream job will reference:
       - for parameters: "{{tasks.<source_job>.outputs.parameters.<output_name>}}"
       - for artifacts: "{{tasks.<source_job>.outputs.artifacts.<output_name>}}"
       where the default output_name is "result" if not explicitly provided.
