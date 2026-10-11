# Required CREATED-task shim cleanup

Guardian supports containerd Runtime v2 implementations whose tasks may be created before networking completes. If setup fails before `Start`, the task remains in `CREATED` state.

For `io.containerd.runsc.v1`, the standard sequence `Kill(SIGKILL) -> Wait -> Delete` does not converge for such a task. The kill request succeeds, but no terminal task event arrives and `Wait` remains blocked. This behavior was reproduced against containerd 2.3.0 and runsc `release-20261005.0-3-g0e57dfe144cb` using the production settings `overlay2=none` and `directfs=false`.

`Nerd.Delete` therefore uses a required alternative-runtime cleanup path for CREATED tasks:

1. Read the task PID from containerd.
2. Verify through `/proc` that its parent is the dedicated runtime shim for that task.
3. Terminate the verified parent shim.
4. Wait until containerd no longer exposes the task.

`Nerd.Delete` owns task deletion only. The existing `RemoveBundle` phase owns container metadata removal.

This is supported product behavior and must remain enabled while the standard runsc/containerd lifecycle cannot produce a terminal event for a pre-start CREATED task.

## Validation

The focused Linux integration test is runtime-selectable through `NERD_TEST_RUNTIME_TYPE`. For runsc it bind-mounts the rootfs to match Garden semantics, confirms the task is `CREATED`, calls `Nerd.Delete`, and asserts that the task disappears. On cgroup v2, it then removes the exact empty cgroup created by the isolated fixture; cgroup teardown is outside `Nerd.Delete` ownership.

The final required-path regression passed on the dedicated gVisor cell: 1 selected spec passed in 5.675 seconds with containerd 2.3.0 and the delivered runsc runtime. The static test binary SHA-256 was `75f36638bd99fb215a16c73c4166f30ccdee9f3328b561726771704a5fcf8fc8`.

The standard lifecycle was also tested without the required path. `Create` succeeded, `State` returned `created`, and runsc accepted `Kill(SIGKILL)`, but `Wait` did not complete within two minutes. Host cleanup through the verified parent-shim boundary restored the cell without residue.

## Removal criteria

Reconsider this path only after the same integration test proves that the standard Runtime v2 sequence converges for a CREATED runsc task and leaves no task, shim, runsc process, cgroup, or bundle residue.