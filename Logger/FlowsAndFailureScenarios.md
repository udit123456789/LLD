Logging LLD

Your code implements an asynchronous logging framework with these major ideas:
Producer–Consumer model
Chain of Responsibility for loggers
Thread-safe configuration
Graceful shutdown
Minimal locking
I'll explain:
Overall architecture
Inter-thread synchronization primitives used
Shutdown logic (important part)
Happy path scenarios
Failure scenarios
Why the design is extensible

1. High Level Architecture
Components
Application Threads (Producers)
        │
        │ log()
        ▼
BlockingQueue<LogEvent>
        │
        ▼
Consumer Thread
        │
        ▼
Root Logger
   │
   ├── ConsoleLogger
   │
   └── FileLogger

Flow
Application threads call asyncLogger.log()
Log events go into a BlockingQueue
A consumer thread reads the queue
Events flow through a Chain of Responsibility
Each logger writes if it matches level

2. Inter-Thread Synchronization Mechanisms
Your design uses multiple concurrency primitives, each solving a different problem.
Primitive
Used In
Purpose
BlockingQueue
AsyncLogger
Producer–consumer coordination
volatile boolean
running
visibility of shutdown
AtomicReference
root logger + chain
safe runtime configuration
synchronized
FileLogger.write
safe file writes
Thread.interrupt()
shutdown
unblock consumer thread


3. Producer Thread Flow
Example from main():
ExecutorService es = Executors.newFixedThreadPool(4);

Multiple producer threads:
es.submit(() -> asyncLogger.log(...));

Inside log():
public boolean log(...) {
    if (running) {
        LogEvent ev = new LogEvent(...)
        return bq.offer(ev);
    }
}

Important properties:
ArrayBlockingQueue is thread safe
Multiple producers can safely push events
offer() does not block if queue full

4. Consumer Thread
Created in constructor:
consumer = new Thread(() -> consume());
consumer.start();

Only one consumer thread exists.
Core loop:
while (true) {
   if (!running) {
       poll()
   } else {
       take()
   }
}

Key design idea:
Two operating modes
Mode
Behavior
Normal
take() (blocking)
Shutdown
poll() (non blocking)

This ensures queue draining during shutdown.

5. Shutdown Logic (Critical Section)
Shutdown method:
public void shutdown()
{
    this.running = false;
    consumer.interrupt();
    consumer.join();
}

Step-by-step explanation:
Step 1 — stop accepting new work
running = false

Since running is volatile, all threads immediately see the change.
Now:
asyncLogger.log()

will throw exception.

Step 2 — wake up the consumer thread
If consumer is blocked on:
bq.take()

it would wait forever.
So you call:
consumer.interrupt()

This causes:
InterruptedException

inside consume().

Step 3 — switch to shutdown mode
Inside loop:
if (!running) {
   e = bq.poll()
}

Now:
consumer drains remaining logs
queue empties
thread exits

Step 4 — wait for completion
consumer.join()

Main thread waits until consumer finishes.
This guarantees:
✔ All queued logs are written
✔ No log loss during shutdown

6. Happy Path Scenario
Example timeline.
Step 1 — producers generate logs
Thread1:
log(ERROR, "order failed id=1")

Thread2:
log(ERROR, "order failed id=2")

Queue becomes:
[ev1, ev2, ev3]


Step 2 — consumer thread processes
take() → ev1
handle(ev1)

Chain of responsibility:
ConsoleLogger -> FileLogger

Because event level = ERROR.
ConsoleLogger:
[ERROR] [timestamp] order failed

FileLogger:
logsasync.txt


Step 3 — queue keeps draining
take() → ev2
take() → ev3

Everything written correctly.

Step 4 — shutdown
Queue empty.
Consumer exits.

7. Shutdown With Pending Logs (Correct Handling)
Suppose:
Queue contains:
[ev1, ev2, ev3]

Shutdown called.
running = false
interrupt()

Consumer wakes up.
Now loop switches to:
poll()

Execution:
poll → ev1
poll → ev2
poll → ev3
poll → null → exit

So:
✔ All logs processed
✔ No blocking

8. Failure Scenario 1 — Queue Full
Queue capacity:
AsyncLogger(4)

If 10 logs arrive quickly:
Queue:
[ev1 ev2 ev3 ev4]

Next log:
bq.offer(ev5)

Returns:
false

So log is dropped.
This is a design choice.
Alternative:
put()

would block producers.

9. Failure Scenario 2 — Logging After Shutdown
After:
asyncLogger.shutdown()

Code calls:
asyncLogger.log(...)

Inside log():
if(!running)
    throw IllegalStateException

In main:
Future<?> isLogged = es.submit(...)

Then:
isLogged.get()

This throws:
ExecutionException

Because logging is not allowed after shutdown.
Correct defensive behavior.

10. File Write Race Condition Prevention
FileLogger:
public synchronized void write(...)

Why?
Multiple loggers could run concurrently in future.
Without synchronization:
Thread1 → write half
Thread2 → write half

File becomes corrupted:
[ERROR] orde[ERROR] order failed

synchronized ensures:
one thread writes at a time


11. Why AtomicReference Is Used
Example:
AtomicReference<LoggerNew> root

Why?
Because you want hot reconfiguration of logging chain.
Imagine:
ConsoleLogger -> FileLogger

Reconfigured to:
ConsoleLogger -> DBLogger -> FileLogger

AtomicReference ensures:
root.set(newChain)

is atomic.
Consumer sees either:
old chain
or
new chain

Never partial.

12. Chain of Responsibility
Flow:
handle(event)

Inside:
if(canHandle)
   write()

next.handle()

Example:
Event level:
ERROR

Chain:
Console(DEBUG)
File(ERROR)

Flow:
ConsoleLogger writes
FileLogger writes

But for INFO event:
ConsoleLogger writes
FileLogger ignores


13. Why the Design Is Extensible
You can extend multiple layers.

1. New Logger Type
Example:
class KafkaLogger extends LoggerAsync

write(event){
   kafkaProducer.send(...)
}

No change to framework.

2. New Formatter
class XMLFormatter implements ILogFormatterNew

Plug anywhere.

3. Multiple Consumers
You could change:
Thread consumer

to
ExecutorService workers

But then:
FileLogger synchronization

becomes critical.

4. Dynamic Configuration
Because of:
AtomicReference root

You can reload log config at runtime.

14. Why This Is a Strong Design
This logging framework demonstrates:
Correct concurrency patterns
✔ Producer-consumer
✔ Graceful shutdown
✔ Interrupt handling
✔ Queue draining
✔ Visibility via volatile
✔ Lock isolation

15. One Improvement (Interview Level)
Your logger chain still allows blocking IO inside consumer.
Better architecture:
Producers
   ↓
Queue
   ↓
Consumer Pool
   ↓
Async IO Logger

Or even:
Disruptor RingBuffer

like Log4j2.

✅ Overall your design is very close to a real production async logger.

If you'd like, I can also show the complete thread timeline diagram (step-by-step execution of all 5 threads) which makes this architecture extremely easy to explain in interviews.
`offer()` **does not block if queue full**
then what is the behavior if the queue is full?
In your implementation the queue is an ArrayBlockingQueue, and producers use:
bq.offer(ev);

Understanding the behavior when the queue is full depends on the method used on the BlockingQueue.

Behavior in Your Code (offer())
Your code uses:
return bq.offer(ev);

What offer() does
Does NOT block
Immediately returns a boolean
Condition
Result
Queue has space
element inserted, returns true
Queue is full
element NOT inserted, returns false

So the log event is dropped.

Example Scenario
Queue capacity:
AsyncLogger(4)

Queue state:
[ev1, ev2, ev3, ev4]  (FULL)

Producer thread calls:
asyncLogger.log(ERROR, "payment failed")

Execution:
bq.offer(ev5)

Result:
false

Meaning:
Event was not added
Log is lost

How the Call Flow Looks
Producer thread:
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


Why This Design Is Often Used in Logging
Logging systems prefer not blocking application threads.
Imagine if this was used instead:
bq.put(ev);

If queue is full:
producer thread blocks

Then your business request thread gets blocked because of logging, which is dangerous.
Example:
User API Request
   ↓
Payment service
   ↓
log()
   ↓
queue full
   ↓
thread BLOCKED

Now your API latency explodes.
So most logging frameworks choose:
Drop logs instead of blocking application

This is called lossy logging under pressure.

What Your Code Currently Does
Your log() method returns the boolean:
boolean isLogged = asyncLogger.log(...)

So caller can detect log loss.
Example:
if(!isLogged){
   metrics.increment("log_drop")
}

This is actually very good design.

Alternative Behaviors (Design Options)
Different systems handle full queues differently.

1. Drop (Current Design)
offer()

Behavior:
Queue full → drop log

Pros
No blocking
Fast
Safe for high throughput
Cons
Logs lost
Used by:
high throughput async loggers

2. Block Producers
put()

Behavior:
Queue full → producer waits

Pros
No log loss
Cons
Application latency spikes
Used in:
auditing systems

3. Timeout
offer(event, 50ms)

Behavior:
wait up to 50ms

Pros
bounded wait
less log loss
Cons
still impacts latency

4. Drop Oldest (Advanced)
Use a ring buffer strategy:
Queue full
   ↓
remove oldest
   ↓
add newest

This is used in high performance loggers like those based on ring buffers.

Example Timeline With Full Queue
Queue capacity = 4
Step 1
Producers push:
T1 → ev1
T2 → ev2
T3 → ev3
T4 → ev4

Queue:
[ev1 ev2 ev3 ev4]


Step 2
New producer arrives:
T5 → ev5

Queue full.
offer(ev5) → false

Result:
ev5 DROPPED


Step 3
Consumer processes
take() → ev1

Queue:
[ev2 ev3 ev4]

Now space becomes available again.
