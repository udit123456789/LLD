# Async Logging Framework -- Queue Full Behavior Explanation

## Overview

In the provided asynchronous logging framework, log events produced by
application threads are pushed into a bounded queue:

``` java
ArrayBlockingQueue<LogEvent> bq;
```

Producer threads add log events using:

``` java
bq.offer(ev);
```

The queue has a **fixed capacity**, meaning that at some point it may
become **full**. Understanding what happens when the queue is full is
important for designing reliable asynchronous systems.

------------------------------------------------------------------------

# Behavior When the Queue is Full

Because the implementation uses:

``` java
offer()
```

the behavior is **non‑blocking**.

### What `offer()` Does

  Condition         Result
  ----------------- -----------------------------------------
  Queue has space   Event inserted and `true` returned
  Queue is full     Event NOT inserted and `false` returned

So when the queue becomes full:

**The log event is dropped.**

------------------------------------------------------------------------

# Example Scenario

Queue capacity:

    AsyncLogger(4)

Queue state:

    [ev1, ev2, ev3, ev4]  (FULL)

Producer thread calls:

``` java
asyncLogger.log(ERROR, "payment failed")
```

Execution:

``` java
bq.offer(ev5)
```

Result:

    false

Meaning:

-   Event **was not added**
-   Log **is lost**

------------------------------------------------------------------------

# Call Flow When Queue is Full

    Application Thread
          │
          ▼
    asyncLogger.log()
          │
          ▼
    queue.offer(event)
          │
          ├── success → event queued
          │
          └── false → queue full → event dropped

------------------------------------------------------------------------

# Why Logging Systems Often Use This Design

Logging should **not slow down business logic**.

Imagine if logging used:

``` java
bq.put(ev)
```

### `put()` Behavior

    Queue full → producer thread blocks

This creates a dangerous situation:

    User API Request
         ↓
    Business Logic
         ↓
    log()
         ↓
    Queue full
         ↓
    Thread BLOCKED

Now your application becomes slow **because of logging**, which is
unacceptable in most systems.

Therefore many asynchronous loggers choose:

    Drop logs instead of blocking application threads

This is called:

**Lossy Logging Under Pressure**

------------------------------------------------------------------------

# Detecting Dropped Logs

Your `log()` method returns a boolean:

``` java
boolean isLogged = asyncLogger.log(...)
```

This allows callers to detect when logging fails.

Example:

``` java
if(!isLogged){
   metrics.increment("log_drop")
}
```

Tracking dropped logs is useful for monitoring **logging backpressure**.

------------------------------------------------------------------------

# Alternative Queue‑Full Strategies

Different systems use different policies.

## 1. Drop Logs (Current Implementation)

    offer()

Behavior:

    Queue full → drop log

Pros:

-   Fast
-   Non‑blocking
-   Protects application performance

Cons:

-   Logs may be lost

------------------------------------------------------------------------

## 2. Block Producers

    put()

Behavior:

    Queue full → producer waits

Pros:

-   No log loss

Cons:

-   Can increase application latency

Common in:

-   audit systems
-   financial transaction logging

------------------------------------------------------------------------

## 3. Timeout Strategy

    offer(event, 50ms)

Behavior:

    wait up to 50ms

Pros:

-   bounded waiting
-   fewer dropped logs

Cons:

-   still affects latency

------------------------------------------------------------------------

## 4. Drop Oldest (Ring Buffer Strategy)

Behavior:

    Queue full
       ↓
    remove oldest event
       ↓
    insert newest event

Used in high‑performance logging systems using **ring buffers**.

------------------------------------------------------------------------

# Example Timeline With Full Queue

Queue capacity = 4

### Step 1 -- Producers Add Logs

    T1 → ev1
    T2 → ev2
    T3 → ev3
    T4 → ev4

Queue:

    [ev1 ev2 ev3 ev4]

------------------------------------------------------------------------

### Step 2 -- Another Producer Attempts Logging

    T5 → ev5

Queue is full.

    offer(ev5) → false

Result:

    ev5 DROPPED

------------------------------------------------------------------------

### Step 3 -- Consumer Processes Logs

Consumer thread:

    take() → ev1

Queue becomes:

    [ev2 ev3 ev4]

Space is now available again.

------------------------------------------------------------------------

# Improvement: Track Dropped Logs

A common improvement is tracking dropped logs.

Example:

``` java
AtomicLong droppedLogs = new AtomicLong();
```

Inside the `log()` method:

``` java
boolean success = bq.offer(ev);
if(!success){
    droppedLogs.incrementAndGet();
}
return success;
```

This helps detect when the logging system is **under heavy load**.

------------------------------------------------------------------------

# Summary

When the queue is full in this async logging design:

-   `offer()` **does not block**
-   It **returns false**
-   The **log event is dropped**
-   Application threads **continue executing normally**

This design protects the **latency and stability of the application**,
which is why many production logging systems follow a similar approach.
