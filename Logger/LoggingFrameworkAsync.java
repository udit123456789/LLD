
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
enum LogLevelDiff
{
    DEBUG(1), INFO(2), WARN(3), ERROR(4);

    final int priority;
    LogLevelDiff(int p)
    {
        this.priority = p;
    }
}

interface ILogFormatterNew
{
    String format(LogEvent event);
}

class LogEvent {
    LogLevelDiff level;
    String message;
    Long timestamp;

    public LogEvent()
    {

    }

    public LogEvent(LogLevelDiff ll, String msg, Long time)
    {
        this.level = ll;
        this.message = msg;
        this.timestamp = time;
    }
}

class SimplePlainTextFormatterNew implements ILogFormatterNew {

    @Override
    public String format(LogEvent event) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(event.level);
        sb.append("] ");
        sb.append("[");
        sb.append(event.timestamp);
        sb.append("] ");
        sb.append(event.message);
        return sb.toString();
    }
}

class JsonFormatterNew implements ILogFormatterNew {

    @Override
    public String format(LogEvent event) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"level\" : ");
        sb.append(event.level);
        sb.append(",");
        sb.append("\"timestamp\" :");
        sb.append(event.timestamp);
        sb.append(",");
        sb.append("\"message\" :");
        sb.append(event.message);
        sb.append("}");
        return sb.toString();
    }
}

abstract class LoggerNew {
    protected LogLevelDiff level;

    public LoggerNew(LogLevelDiff level) {
        this.level = level;
    }

    // but this will not work for multi threaded environment so we create another class.
    private LoggerNew next;

    protected void setNextLogger(LoggerNew logger)
    {
        this.next = logger;
    }

    protected boolean canHandle(LogEvent event)
    {
        return event.level.priority >= level.priority;
    }

    public void handle(LogEvent event)
    {
        if(canHandle(event))
        {
            write(event);
        }

        if(this.next != null)
        {
            this.next.handle(event);
        }
    }

    protected abstract void write(LogEvent event);
}

class ConsoleLogger extends LoggerNew
{
    private ILogFormatterNew formatter;

    public ConsoleLogger(ILogFormatterNew formatter, LogLevelDiff ll)
    {
        super(ll);
        this.formatter = formatter;
    }

    @Override
    public void write(LogEvent event) {
        System.out.println(formatter.format(event));
    }
}

class FileLogger extends LoggerNew 
{

    private ILogFormatterNew formatter;
    private String fileName;
    private Writer writer;

    public FileLogger(ILogFormatterNew formatter, String fileName, LogLevelDiff ll)
    {
        super(ll);
        this.formatter = formatter;
        this.fileName = fileName;
        try {
            this.writer = new BufferedWriter(new FileWriter(fileName, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Using synchronized provides the following guarantees:
    //✔ Guarantees atomic writes
    //✔ Prevents interleaving
    //✔ Lock scope is minimal
    @Override
    public synchronized void write(LogEvent event) {
        try {
            this.writer.write(this.formatter.format(event));
            this.writer.write("\n");
            this.writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

enum AsyncLoggerState
{
    RUNNING,
    SHUTTING_DOWN,
    TERMINATED
}

class AsyncLogger
{
    private AtomicReference<LoggerNew> rootLogger;
    private AtomicReference<AsyncLoggerState> state;
    private AtomicInteger activeProducers;
    // using blocking queue will make the producers write in thread safe manner and also blocks the queue in case queue is full.
    private BlockingQueue<LogEvent> bq;

    // currently we are using a single consumer thread but we can have thread pool here as well
    // just that it will need synchronization at the consume level to avoid threads corrupting each other's processed log messages.
    private Thread consumer;

    public AsyncLogger(int cap, LoggerNew rl)
    {
        bq = new ArrayBlockingQueue<>(cap);
        this.rootLogger = new AtomicReference<>(rl);
        consumer = new Thread(() -> consume());
        state = new AtomicReference<AsyncLoggerState>(AsyncLoggerState.RUNNING);
        activeProducers = new AtomicInteger(0);
        consumer.start();
    }

    public boolean log(LogLevelDiff ll, String message)
    {
        if(state.get() == AsyncLoggerState.RUNNING)
        {
            activeProducers.incrementAndGet();
            try
            {
                if(state.get() != AsyncLoggerState.RUNNING)
                {
                    return false;
                }
                LogEvent ev = new LogEvent(ll, message, Instant.now().toEpochMilli());
                return this.bq.offer(ev);
            }
            finally{
                activeProducers.decrementAndGet();
            }
        }
        else
        {
            throw new IllegalStateException("Can't submit a task after shutdown.");
        }
    }

    private void consume()
    {
        // this is by default thread safe as only one worker thread is reading from the queue i.e hitting the handler.
        // but if we increase the worker threads > 1 maybe via thread pool then this can cause issues, so better to handle at handler level.
        // while (running || !bq.isEmpty()) {
        //     try {
        //             LogEvent e = bq.take();
        //             this.rootLogger.get().handle(e);
        //     } catch (InterruptedException e) {
        //         Thread.currentThread().interrupt();
        //     }
        // }

        boolean interrupted = false;

        while (true) {
            try {
                /*
                    We could have used but this wastes CPU cycles
                    LogEvent e = bq.poll(); ot it's timeout variant

                    So we can take a hybrid approach where we use take while state is running and then switch to poll when SHUTTING_DOWN.
                */
                LogEvent ev;
                if (state.get() == AsyncLoggerState.RUNNING) {
                    // Efficient blocking
                    ev = bq.take();
                } else {
                    // Drain queue during shutdown
                    ev = bq.poll();

                    if (ev == null &&
                        activeProducers.get() == 0) {
                        break;
                    }
                }

                if (ev != null) {
                    rootLogger.get().handle(ev);
                    continue;
                }

            } catch (InterruptedException ie) {
                interrupted = true;
            }
        }

        state.set(AsyncLoggerState.TERMINATED);

        if (interrupted) {
            Thread.currentThread().interrupt(); // restore flag
        }
    }

    public void shutdown()
    {
        if (!state.compareAndSet(AsyncLoggerState.RUNNING, AsyncLoggerState.SHUTTING_DOWN)) {
            return;
        }
        consumer.interrupt();

        try {
            consumer.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

class LoggerHandlerAsync
{
    // make this as well atomic reference to avoid partial state.
    private AtomicReference<LoggerNew> root;

    private LoggerHandlerAsync()
    {
        root = new AtomicReference<>();
    }

    public static LoggerHandlerAsync getInstance()
    {
        return HOLDER.instance;
    }

    private static final class HOLDER
    {
        private static final LoggerHandlerAsync instance = new LoggerHandlerAsync();
    }

    public void setRoot(LoggerNew r)
    {
        this.root.set(r);
    }

    public LoggerNew getRoot()
    {
        return this.root.get();
    }

    public void configure(List<LoggerNew> loggers)
    {
        for(int i = 0; i <loggers.size()-1; i++)
        {
            loggers.get(i).setNextLogger(loggers.get(i+1));
        }
        loggers.get(loggers.size()-1).setNextLogger(null);
        //Atomic swap
        root.set(loggers.get(0));
    }
}


public class LoggingFrameworkAsync {
    public static void main(String[] args) {
        ILogFormatterNew plaintextformatter = new SimplePlainTextFormatterNew();
        LoggerNew consoleLogger = new ConsoleLogger(plaintextformatter, LogLevelDiff.DEBUG);
        LoggerNew fileLogger = new FileLogger(plaintextformatter, "logsasync.txt", LogLevelDiff.ERROR);

        LoggerHandlerAsync.getInstance().configure(Arrays.asList(consoleLogger, fileLogger));
        AsyncLogger asyncLogger = new AsyncLogger(4, LoggerHandlerAsync.getInstance().getRoot());

        ExecutorService es = Executors.newFixedThreadPool(4);
        for(int i = 0; i < 4; i++)
        {
            int idx = i;
            es.submit(() -> {asyncLogger.log(LogLevelDiff.ERROR, "order failed unexpectedly with id = " + idx);});
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        asyncLogger.shutdown();

        Future<?> isLogged = es.submit(() -> {asyncLogger.log(LogLevelDiff.ERROR, "submission after shutdown.");});

        try {
            System.out.println("is submitted : " + isLogged.get());
        } catch (InterruptedException | ExecutionException e) {
            System.out.println(e.getMessage());
        }

        es.shutdown();
        try {
            es.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

    }
}
