
def apply_constraints(workflow, jobs):
    """
    Modify the workflow to add custom scheduling constraints:
    
    - Set the global parallelism to 5.
    - Limit concurrent slurm jobs to 2 by chaining them into two serial groups.
    """
    # Set global parallelism for the entire workflow.
    workflow["spec"]["parallelism"] = 5

    # Identify slurm job names
    slurm_job_names = {job["name"] for job in jobs if job["type"] == "slurm"}
    
    # Locate the DAG template
    dag_template = None
    for template in workflow["spec"].get("templates", []):
        if "dag" in template:
            dag_template = template
            break

    if not dag_template:
        return workflow  

    tasks = dag_template["dag"].get("tasks", [])

    # Filter tasks to only those corresponding to slurm jobs.
    slurm_tasks = [task for task in tasks if task["name"] in slurm_job_names]

    # Group the slurm tasks into 2 groups (e.g. alternating assignment).
    group0 = []
    group1 = []
    for idx, task in enumerate(slurm_tasks):
        if idx % 2 == 0:
            group0.append(task)
        else:
            group1.append(task)

    # Helper function to chain tasks within a group.
    def chain_tasks(task_group):
        for i in range(1, len(task_group)):
            prev_task_name = task_group[i - 1]["name"]
            # If the task already has dependencies, add the new one.
            if "dependencies" in task_group[i]:
                if prev_task_name not in task_group[i]["dependencies"]:
                    task_group[i]["dependencies"].append(prev_task_name)
            else:
                task_group[i]["dependencies"] = [prev_task_name]

    # Chain tasks in each group sequentially.
    chain_tasks(group0)
    chain_tasks(group1)

    return workflow
