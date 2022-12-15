package tributary;

public class RoundRobin<E> implements RebalanceStrategy<E> {

    public RoundRobin() {
    }

    public void rebalance(Topic<E> topic, ConsumerGroup<E> group) {
        group.resetConsumerAllocations();
        int numConsumers = group.getConsumers().size();
        int numPartitions = topic.getPartitions().size();
        for (int i = 0; i < numPartitions; i++) {
            Consumer<E> consumer = group.getConsumers().get(i % numConsumers);
            consumer.addPartition(topic.getPartitions().get(i));
        }
        return;
    }

}
