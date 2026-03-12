import java.util.*;
import java.util.concurrent.*;

public class MiniKafka {

    /* ---------------- MESSAGE ---------------- */

    public static class Message {

        private final String payload;

        public Message(String payload) {
            this.payload = payload;
        }

        public String getPayload() {
            return payload;
        }
    }

        /* ---------------- TOPIC ---------------- */

    public static class Topic {

        private final String name;

        private final List<Partition> partitions;

        public Topic(String name, int count, int capacity) {

            this.name = name;

            partitions = new ArrayList<>();

            for (int i = 0; i < count; i++) {
                partitions.add(new Partition(capacity));
            }
        }

        public Partition getPartition(int key) {

            // this can be improved further where we can have partitionassignment strategy based on key and partition count
            return partitions.get(Math.abs(key) % partitions.size());
        }

        public List<Partition> getPartitions() {
            return partitions;
        }

        public String getName() {
            return name;
        }

        public Partition getPartitionById(String id)
        {
            for(int i = 0; i < partitions.size(); i++)
            {
                if(partitions.get(i).getId().equals(id))
                {
                    return partitions.get(i);
                }
            }

            return null;
        }
    }

    /* ---------------- PARTITION ---------------- */

    public static class Partition {

        private final String id = UUID.randomUUID().toString();

        private final List<Message> log = new ArrayList<>();

        // this is to have size based cleanup automatically happening, not implemented time based.
        private final int capacity;

        private long baseOffset = 0;

        public Partition(int capacity) {
            this.capacity = capacity;
        }

        public String getId() {
            return id;
        }

        public synchronized long append(Message m) {

            if (log.size() == capacity) {
                log.remove(0);
                baseOffset++;
            }

            log.add(m);

            notifyAll();

            return baseOffset + log.size() - 1;
        }

        public synchronized Message read(long offset) {

            if (offset < baseOffset) {
                throw new RuntimeException("Offset expired");
            }

            int index = (int) (offset - baseOffset);

            if (index >= log.size()) return null;

            return log.get(index);
        }

        public synchronized long endOffset() {
            return baseOffset + log.size();
        }

        public synchronized long baseOffset() {
            return baseOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Partition)) return false;
            Partition p = (Partition) o;
            return id.equals(p.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }

        /* ---------------- TOPIC PARTITION KEY ---------------- */

    public static class TopicPartition {

        private final String topic;
        private final String partitionId;

        public TopicPartition(String topic, String partitionId) {
            this.topic = topic;
            this.partitionId = partitionId;
        }

        public String getTopic()
        {
            return this.topic;
        }

        public String getPartitionId()
        {
            return this.partitionId;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TopicPartition)) return false;
            TopicPartition tp = (TopicPartition) o;
            return topic.equals(tp.topic) &&
                    partitionId.equals(tp.partitionId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partitionId);
        }
    }

    /* ---------------- PRODUCER ---------------- */

    public static class Producer {

        public void publish(Topic topic, String payload) {

            Message m = new Message(payload);

            Partition p = topic.getPartition(payload.hashCode());

            long offset = p.append(m);

            System.out.println(
                    "Produced -> topic=" + topic.getName()
                            + " offset=" + offset
                            + " msg=" + payload
            );
        }
    }

    /* ---------------- CONSUMER ---------------- */

    public interface Consumer {
        void process(Message m);
    }

    public static class SimpleConsumer implements Consumer {

        private final String id;

        public SimpleConsumer(String id) {
            this.id = id;
        }

        public void process(Message m) {

            System.out.println(
                    "Consumer " + id + " -> " + m.getPayload()
            );

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /* ---------------- CONSUMER WORKER ---------------- */

    public static class ConsumerWorker implements Runnable {

        private final Partition partition;
        private final Consumer consumer;
        private final ConsumerGroup group;
        private final TopicPartition tp;

        public ConsumerWorker(
                Partition partition,
                Consumer consumer,
                ConsumerGroup group,
                String topic) {

            this.partition = partition;
            this.consumer = consumer;
            this.group = group;
            this.tp = new TopicPartition(topic, partition.getId());
        }

        @Override
        public void run() {

            try {

                while (!Thread.currentThread().isInterrupted()) {

                    Message m;

                    synchronized (partition) {

                        long offset = group.getOffset(tp);

                        while (offset >= partition.endOffset()) {
                            partition.wait();
                            offset = group.getOffset(tp);
                        }

                        m = partition.read(offset);

                        group.commitOffset(tp, offset + 1);
                    }

                    consumer.process(m);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /* ---------------- WORKER HANDLE ---------------- */

    public static class WorkerHandle {

        private final ConsumerWorker worker;
        private final Future<?> future;

        public WorkerHandle(ConsumerWorker worker, Future<?> future) {
            this.worker = worker;
            this.future = future;
        }

        public void stop() {
            future.cancel(true);
        }
    }

    /* ---------------- CONSUMER GROUP ---------------- */

    public static class ConsumerGroup {

        private final String groupId;

        private final List<Consumer> consumers =
                new CopyOnWriteArrayList<>();

        private final Map<Consumer, List<TopicPartition>> assignments =
                new ConcurrentHashMap<>();

        private final Map<TopicPartition, WorkerHandle> workers =
                new ConcurrentHashMap<>();

        private final Map<TopicPartition, Long> offsets =
                new ConcurrentHashMap<>();

        private final ExecutorService executor;

        public ConsumerGroup(String groupId, ExecutorService executor) {
            this.groupId = groupId;
            this.executor = executor;
        }

        public synchronized void addConsumer(Topic topic, Consumer c) {

            consumers.add(c);

            rebalance(topic);
        }

        public synchronized void removeConsumer(Topic topic, Consumer c) {

            consumers.remove(c);

            rebalance(topic);
        }

        public long getOffset(TopicPartition tp) {

            return offsets.getOrDefault(tp, 0L);
        }

        public void commitOffset(TopicPartition tp, long offset) {

            offsets.put(tp, offset);
        }

        public synchronized void resetOffset(
                Topic topic,
                String partitionId,
                long newOffset) {

            Partition target = null;

            for (Partition p : topic.getPartitions()) {
                if (p.getId().equals(partitionId)) {
                    target = p;
                    break;
                }
            }

            if (target == null) return;

            TopicPartition tp =
                    new TopicPartition(topic.getName(), partitionId);

            offsets.put(tp, newOffset);

            synchronized (target) {
                target.notifyAll();
            }

            System.out.println(
                    "Offset reset -> topic=" + topic.getName()
                            + " partition=" + partitionId
                            + " newOffset=" + newOffset
            );
        }

        private void rebalance(Topic topic) {

            stopWorkers(topic);

            removeAssignmentsForTopic(topic.getName());

            List<Partition> partitions = topic.getPartitions();

            for (int i = 0; i < partitions.size(); i++) {

                Consumer consumer =
                        consumers.get(i % consumers.size());

                assignments
                        .computeIfAbsent(
                                consumer,
                                c -> new ArrayList<>())
                        .add(new TopicPartition(topic.getName(), partitions.get(i).getId()));
            }

            startWorkers(topic);
        }

        private void removeAssignmentsForTopic(String topicName) {

            for (Map.Entry<Consumer, List<TopicPartition>> entry : assignments.entrySet()) {

                List<TopicPartition> list = entry.getValue();

                list.removeIf(tp -> tp.topic.equals(topicName));
            }
        }

        private void stopWorkers(Topic topic) {

            for (Partition p : topic.getPartitions()) {

                TopicPartition tp =
                        new TopicPartition(topic.getName(), p.getId());

                WorkerHandle handle = workers.remove(tp);

                if (handle != null) {
                    handle.stop();
                }
            }
        }

        private void startWorkers(Topic topic) {

            String topicName = topic.getName();

            for (Map.Entry<Consumer, List<TopicPartition>> entry : assignments.entrySet()) {

                Consumer consumer = entry.getKey();

                for (TopicPartition tp : entry.getValue()) {

                    if (!tp.getTopic().equals(topicName)) {
                        continue;
                    }

                    // prevent duplicate worker creation
                    if (workers.containsKey(tp)) {
                        continue;
                    }

                    Partition partition = topic.getPartitionById(tp.getPartitionId());

                    if (partition == null) {
                        continue;
                    }

                    offsets.putIfAbsent(tp, partition.baseOffset());

                    ConsumerWorker worker =
                            new ConsumerWorker(
                                    partition,
                                    consumer,
                                    this,
                                    topicName
                            );

                    Future<?> future = executor.submit(worker);

                    workers.put(tp, new WorkerHandle(worker, future));
                }
            }
        }
    }

    /* ---------------- BROKER ---------------- */

    public static class Broker {

        private final Map<String, Topic> topics =
                new ConcurrentHashMap<>();

        private final Map<String, ConsumerGroup> groups =
                new ConcurrentHashMap<>();

        private final ExecutorService executor =
                Executors.newFixedThreadPool(20);

        public Topic createTopic(String name, int partitions, int capacity) {

            Topic topic = new Topic(name, partitions, capacity);

            topics.put(name, topic);

            return topic;
        }

        public void subscribe(
                String topicName,
                String groupId,
                Consumer consumer) {

            Topic topic = topics.get(topicName);

            ConsumerGroup group =
                    groups.computeIfAbsent(
                            groupId,
                            g -> new ConsumerGroup(g, executor));

            group.addConsumer(topic, consumer);
        }

        public void resetOffset(
                String topicName,
                String groupId,
                String partitionId,
                long offset) {

            ConsumerGroup g = groups.get(groupId);
            if(g == null)
            {
                System.err.print("No group with id = " + groupId + " is present.");
                return;
            }
            Topic topic = topics.get(topicName);

            if (g != null) {
                g.resetOffset(topic, partitionId, offset);
            }
        }

        public void shutdown() {

            executor.shutdownNow();

            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /* ---------------- MAIN ---------------- */

    public static void main(String[] args) throws Exception {

        Broker broker = new Broker();

        Topic orders = broker.createTopic("orders", 3, 100);

        Producer producer = new Producer();

        Consumer c1 = new SimpleConsumer("C1");
        Consumer c2 = new SimpleConsumer("C2");

        System.out.println("\n--- Step 1 : Consumers join ---");

        broker.subscribe("orders", "groupA", c1);
        broker.subscribe("orders", "groupA", c2);

        Thread.sleep(1000);

        System.out.println("\n--- Step 2 : Produce initial messages ---");

        for (int i = 0; i < 6; i++) {
            producer.publish(orders, "Order-" + i);
        }

        Thread.sleep(4000);

        System.out.println("\n--- Step 3 : Add new consumer (rebalance) ---");

        Consumer c3 = new SimpleConsumer("C3");

        broker.subscribe("orders", "groupA", c3);

        Thread.sleep(1000);

        for (int i = 6; i < 10; i++) {
            producer.publish(orders, "Order-" + i);
        }

        Thread.sleep(4000);

        System.out.println("\n--- Step 4 : Reset offset to replay messages ---");

        String partitionId = orders.getPartitions().get(0).getId();

        broker.resetOffset(
                "orders",
                "groupA",
                partitionId,
                0
        );

        System.out.println("Replaying messages from partition 0...");

        Thread.sleep(5000);

        System.out.println("\n--- Shutdown ---");

        broker.shutdown();
    }
}
