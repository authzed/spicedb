# go-rtml

golang real time memory limiter - check for memory pressure in real time to drop or reject (back-pressure) new work items before your go application terminates with OutOfMemory (OOM).

This package is all about the STABILITY of your go application under memory pressure.

## Motivation

Your golang applications need memory (RAM) to run. In an utopic world, you would have infinite memory and never need to think or worry about how much memory resources are consumed. In the real world, memory is a limited resource which has to be managed carefully.

When your process is under memory pressure for any reason, you - the application developer, is responsible to avoid processing new work items which can increase the memory pressure more and lead to OutOfMemory brutal termination of the process.

## How it works

- Make sure you set `GOMEMLIMIT` environment variable in alignment to your container memory limit.
- Call `rtml.IsMemLimitReached()` in the entry points to your application, (or on checkpoint before doing some potentially expensive allocations).
- Do it where you have the ability to reject, drop, or apply back-pressure your senders.
- Prefer to call it as soon as possible, before any expensive allocations are made.

This simple function will tell you if the memory limit is reached and allow you to react, for example: 

- apply back-pressure to the sender by rejecting the work item. they are expected to retry after some time, at which point hopefully, memory pressure has already reduced to normal levels.
- drop the work item if the sender is not able to retry it safely.
- notify some monitoring system about this event for further investigation or initiazte automatic scaling.

## Usage

```go
package main

import (
	rtml "github.com/odigos-io/go-rtml"
)

func requestHandler() {
    if rtml.IsMemLimitReached() {
        return errResourceExhausted, "Memory limit reached"
    }

    // process request, which might be be allocation heavy.
    return nil // success, no memory limit to back-pressure.
}
```

and build your application with the following ldflag: "-checklinkname=0".

## About ldflags="-checklinkname=0"

This package uses `go:linkname` to access the internal state of the go runtime.

This is [considered bad practice and not recommended by the go team](https://github.com/golang/go/issues/67401), thus the ldflag to warn you.

Having said that, it does address a hard to solve real-world problem, in a way that satisfies tight performance requirements and acurate reaction to memory pressure.

Be aware that this practice, while working, can break unexpectedly by internal changes to the go runtime implementation without notice. Test your application with every new go version, weight the benefits, risks and alternatives, and evaluate if this risk is acceptable for you.

We run daily tests for all version of go above 1.23 to ensure that the package is compatible and stable.

## Frequency

Calling `rtml.IsMemLimitReached()` is considered "cheap", since it is doing the same work as go runtime does once every few KBs of heap allocations.

It is ok to call it one every request, but keep in mind that it still need to synchronizly access few atomic variables and do some computations, so it's not free. Prefer calling it on batches if possible and not on every item if there are many of them.

Test your application under load to exaimne it's performance.

## What is Memory Limit?

Memory limit is a feature of the operating system, that will terminate your go process (or group of processes) if they are collectivly consuming more memory than the allowed limit.

It is very common in kuberenetes to set resource request and limit on memory and cpu. Under the hood it uses cgroups which is a linux kernel feature to achieve this goal.

## Why Set Memory Limit?

If you are a developer, setting memory limit is a chore that you probably don't want to participate in. But if you are a devops engineer, you care about the overall stability of the system, and probably learned the easy or hard way that not setting any memory limit is a big no-no and recipe for disaster.

If the application suddenly starts consuming a lot of memory and there are no limits in place, this memory will have to come from somewhere. it will start eating into the operating system's free memory, which can end up in degraded performance, resource exhaustion, and harm other applications running on the same machine.

## Why it's Important to Avoid Crashing due to OOM

Giving that you (or your opeartion engineer) have set memory limit (best practice) - you are guarded against the application causing general system or machine instability which is a good start.
But it creates a new problem - what if the application itself reaches the memory limit and get terminated? How do we guarentee the application stability in this case?

Why it is so important to not crash the application due to OOM?

- Some platforms (like kubernetes) handles automatic autoscaling which can address issues of memory exhaustion, but for it to trigger, the container needs to be up and report metrics. When a container crashes, the relevant metrics might not be recorded in time and prevent the autoscaler from starting more replicas or increasing the resources which ends up in a death spiral.
- In mature production environments, crashing pods will show up as alerts or in dashboards, creating operational noise and alert fatigue.
- Crashing with OOM can cause data loss or corruption since the process is terminated abruptly and does not have a chance to execute a graceful shutdown.
- If the application crashes too often, it can cause service downtime and fail to serve requests in general.
- Degraded user experience, confidence, and trust over time.

For these reasons, we aim to never crash the application due to OOM. Easier said than done :/

## How Check the Limit in Real-Time Works?


### Container Level Memory Limit

Container level memory limit are usually enforced in the operating system, on a cgroup (or in containerized environments, for all processes in the container).

For simplicity, and since go usually runs as a single process (with multiple OS threads), we will assume that our process is the only one in the container, thus it should apply to the set limits as well.

### GOMEMLIMIT

GOMEMLIMIT is a way to reflect the container memory limit to the go runtime and allow it to call the garbage collector in a way to trys to avoid crossing the runtime limit. It is usually set to a value that is a bit lower from the container memory limit to give some headroom for spikes, inacuracies and garbage collection reaction time. 

You can, for example, set GOMEMLIMIT to 80% of the container memory limit, which for 1GB container limit, will leave you with 800MB for normal usage, and 200MB for spikes and safe margins.

This number is somehow arbitrary and encapsulates a trade-off between stability and costs (memory usage).

### Resident Set Memory and "Ready" Memory

The memory that is counted towards the container (cgroup) memory limit is called "resident set" memory, which is kernel memory pages which are backed by physical memory. This is the number the kernel will use to account and trigger OutOfMemory terminations.

Go runtime on it's side, tracks the number of pages it considers as "Ready". A memory page is ready if the runtime can use it to make allocations. A ready page is usually backed by physical memory and contributes to the resident set memory, but not always (the operating system is quite efficient in delaying the physical memory allocation until it is really needed).

Therefore: `ResidentSet` (kernel) <= `MappedReady` (go runtime)

So the first check we do is:

```
if MappedReady < GOMEMLIMIT {
    return false // memory limit not reached
}
```

If the ready memory is less then the limit, then for sure the resident set memory is also below the limit and limit is not reached.

This check alone is sufficient to capture most applications in normal unloaded state and provide a cheap and fast return path almost always.

### HeapFree

When go runtime needs to allocate new kernel memory for it's heap, or when memory is no longer used and can be freed, it calls the operating system api to do so. This call is using a syscall and considered expensive.

After grabage collection, when the runtime is left with unused memory, it will mark it as "free" and will not imidiatly return it to the operating system. "free" memory is still counted as "Ready" in the runtime, and considered part of the resident set memory for the container memory limit.

This memory can be reused for new allocations, but is counted in the HeapReady count, thus we will ignore it for the computation, just like the go garbage collector does when it calculates the heap goal.

```
if (MappedReady - HeapFree) < GOMEMLIMIT {
    return false // memory limit not reached
}
```

This check will signal that there is still memory available for allocations if the HeapFree is high enough.

### HeapGoal

The final check is where things get interesting. Go runtime will honor the GOMEMLIMIT environment variable and will try to keep the heap size below it by triggering garbage collection. 

In the good scenario, the memory usages grows and garbage collection is triggered right near the GOMEMLIMIT. After garbage collection, there is now a lot of free memory, to use for future allocations.

In the bad scenario, your program is still holding references to this memory and after running garbage collection, the consumed memory is still above the GOMEMLIMIT.

Quoting from the go runtime source code comments:

> There's honestly not much we can do here but just trigger GCs continuously
> and let the CPU limiter reign that in. Something has to give at this point.

This is where it's most critical to reject any new allocations and not add up new allocations to the heap when we are already above the GOMEMLIMIT.

The way we check it is - by doing exactly what go runtime is doing. We calculate the heap goal and compare it with heap live.

```
if HeapLive < HeapGoal {
    return false // memory limit not reached
}
```

This check signals that go GC controller has not got to a point where collection is required, thus new allocations are safe at this point.

Otherwise, If both HeapReady is above the GOMEMLIMIT, HeapFree does not help, and the allocations counts indicate garbage collection cannot go below the goal, then we know that the memory limit is reached.


