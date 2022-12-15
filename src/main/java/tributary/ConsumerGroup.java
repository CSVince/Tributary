package tributary;

import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup<E> {
    private String id;
    private RebalanceStrategy<E> rebalancingStrategy;
    private List<Consumer<E>> consumers = new ArrayList<>();

    /**
     * Constructor for consumer group
     * @param id unique identifier
     * @param strategy strategy for rebalancing
     * @return new consumer group
     */
    public ConsumerGroup(String id, String strategy) {
        this.id = id;
        switch (strategy) {
            case "Range":
                this.rebalancingStrategy = new Range<E>();
                break;
            case "RoundRobin":
                this.rebalancingStrategy = new RoundRobin<E>();
                break;
            default:
                this.rebalancingStrategy = new Range<E>();
                break;
        }
    }

    /**
     * Gets the ID of the consumer group
     * @return String corresponding to ID
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the list of consumers in this group
     * @return list of consumers in this consumer group
     */
    public List<Consumer<E>> getConsumers() {
        return consumers;
    }

    /**
     * Add a consumer to the list of consumers
     * @param consumerId unique identifier for the new consumer
     * @pre the consumerId is unique
     * @post there is a new consumer with the specified id in this group
     * @return the newly created consumer
     */
    public synchronized Consumer<E> addConsumer(String consumerId) {
        Consumer<E> newCons = new Consumer<E>(consumerId);
        consumers.add(newCons);
        return newCons;
    }

    /**
     * Delete a consumer from this counsumer group
     * @param consumerId unique ID of the consumer to be removed
     * @pre a consumer with the specified id exists in this group
     * @post the consumer with the specified id no longer exists in this group
     * @return void
     */
    public synchronized void deleteConsumer(String consumerId) {
        Consumer<E> consToBeDeleted;
        try {
            consToBeDeleted = consumers.stream()
                                        .filter(consumer -> consumer.getId()
                                        .equals(consumerId))
                                        .findFirst()
                                        .get();
        } catch (Exception e) {
            return;
        }
        consumers.remove(consToBeDeleted);
    }

    /**
     * Checks whether this consumer group contains a specific consumer
     * @param consumerId unique identifier of consumer
     * @return boolean corresponding to whether consumer is present in this group
     */
    public boolean containsConsumer(String consumerId) {
        return consumers.stream().anyMatch(consumer -> consumer.getId().equals(consumerId));
    }

    /**
     * Gets a consumer with a specified ID in this group
     * @param consumerId unique identifier of consumer
     * @return consumer with specified ID
     */
    public Consumer<E> getConsumer(String consumerId) {
        return consumers.stream().filter(consumer -> consumer.getId().equals(consumerId)).findFirst().get();
    }

    /**
     * Displays the consumer group details (including its consumers)
     * @return void
     */
    public void display() {
        System.out.println("Now displaying consumer group with ID: " + id);
        consumers.stream()
                 .forEach(consumer -> {
                    System.out.println("Consumer ID: " + consumer.getId());
                    System.out.println("Partition Receiving ID: ");
                    consumer.displayPartitions();
                 });
    }

    /**
     * Sets the rebalancing strategy for this consumer group
     * @param strategy rebalancing strategy
     * @pre the strategy is a valid strategy
     * @post the rebalancing strategy of this group is set to the strategy parameter passed in
     * @return void
     */
    public void setRebalancingStrategy(String strategy) {
        switch (strategy) {
            case "Range":
                this.rebalancingStrategy = new Range<E>();
                break;
            case "RoundRobin":
                this.rebalancingStrategy = new RoundRobin<E>();
                break;
            default:
                this.rebalancingStrategy = new Range<E>();
                break;
        }
    }

    /**
     * Rebalances the consumer allocations in the group for a specific topic
     * @param topic unique identifier for a topic
     * @pre the topic is a valid topic
     * @post the consumers in this group are reallocated properly according to the strategy
     * @return void
     */
    public void rebalance(Topic<E> topic) {
        if (consumers.size() == 0) return;
        rebalancingStrategy.rebalance(topic, this);
    }

    /**
     * Resets the consumer allocations for this consumer group
     * @post the consumers in this group are unallocated all their partitions
     * @return void
     */
    public void resetConsumerAllocations() {
        consumers.stream().forEach(c -> c.resetAllocation());
    }
}
