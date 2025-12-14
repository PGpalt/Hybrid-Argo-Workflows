
def apply(api):
    """
    Example policy:
      - Set global parallelism to 5.
      - Split SLURM tasks into two groups (alternating assignment), then chain each group. 
      - Enforcing a parallelism constraint for slurm jobs
    """
    # Global knob
    api.set_global_parallelism(5)

    # Alternating partition of SLURM tasks
    g0, g1 = [], []
    for idx, t in enumerate(api.slurm_tasks):
        (g0 if idx % 2 == 0 else g1).append(t["name"])

    # Chain each lane
    api.chain(g0)
    api.chain(g1)
