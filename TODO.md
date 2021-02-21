Scheduling routine
1. Select tasks ready to launch
1. Select workers ready to take work
1. Produce a number of invocations (support exchangeable algorithms)
---
Worker tracking routine
1. Select all workers
1. Try exchange messages with them
1. mark non-responding as suspicious
1. treat suspicious according to policy.
    1. unresponsive: mark worker as offline, mark invocation as failed
    1. responsive: remove suspicion
---

