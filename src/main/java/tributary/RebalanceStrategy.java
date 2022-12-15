package tributary;

public interface RebalanceStrategy<E> {
    public void rebalance(Topic<E> topic, ConsumerGroup<E> group);
}
