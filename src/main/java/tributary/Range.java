package tributary;

public class Range<E> implements RebalanceStrategy<E> {
    public Range() {
    }

    public void rebalance(Topic<E> topic, ConsumerGroup<E> group) {
        group.resetConsumerAllocations();
        int numConsumers = group.getConsumers().size();
        int numPartitions = topic.getPartitions().size();
        int numPartPerCons = numPartitions / numConsumers;
        if (numPartitions % numConsumers != 0) {
            numPartPerCons++;
        }
        int counterParts = 0;
        int counterCons = 0;
        for (int i = 0; i < numPartitions; i++) {
            Consumer<E> consumer = group.getConsumers().get(counterCons);
            consumer.addPartition(topic.getPartitions().get(i));
            counterParts++;
            if (counterParts == numPartPerCons) {
                if (counterCons == 0 && numPartitions % numConsumers != 0) {
                    numPartPerCons--;
                }
                counterParts = 0;
                counterCons++;
            }
        }
        return;
    }
}
