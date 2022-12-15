package tributary;

import java.util.ArrayList;
import java.util.List;

public class Topic<E> {

    private String id;
    private List<Partition<E>> partitions;
    private List<ConsumerGroup<E>> consumerGroups;

    /**
     * Constructor for topic
     * @param id identifier for topic
     * @return new topic
     */
    public Topic(String id) {
        this.id = id;
        this.partitions = new ArrayList<>();
        this.consumerGroups = new ArrayList<>();
    }

    /**
     * Gets the id of the topic
     * @return String corresponding to topic ID
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the partitions contained in the topic
     * @return list of partitions
     */
    public List<Partition<E>> getPartitions() {
        return partitions;
    }

    /**
     * Gets the consumer groups assigned to the topic
     * @return list of consumer groups
     */
    public List<ConsumerGroup<E>> getConsumerGroups() {
        return consumerGroups;
    }

    /**
     * Adds a partition to this topic
     * @param partitionId unique identifier for the new partition
     * @pre the partitionId is unique
     * @post there is a new partition with the specified id in the topic
     * @return the newly created partition
     */
    public synchronized Partition<E> addPartition(String partitionId) {
        Partition<E> newPartition = new Partition<E>(partitionId);
        partitions.add(newPartition);
        return newPartition;
    }

    /**
     * Adds a consumer group to this topic
     * @param groupId unique identifier for the new consumer group
     * @param strategy rebalancing strategy for the consumer group
     * @pre the groupId corresponds to a valid group. the strategy is a valid strategy.
     * @post there is a new consumer group with the specified id in the topic.
     * @return the newly created consumer group
     */
    public synchronized ConsumerGroup<E> addConsumerGroup(String groupId, String strategy) {
        ConsumerGroup<E> newGroup = new ConsumerGroup<>(groupId, strategy);
        consumerGroups.add(newGroup);
        return newGroup;
    }

    /**
     * Adds a consumer to the specified consumer group
     * @param groupId unique identifier for the consumer group
     * @param consumerId unique identifier for the new consumer
     * @pre the groupId corresponds to a valid group. the consumerId is valid
     * @post there is a new consumer in the consumer group with the specified id
     * @return the newly created consumer
     */
    public synchronized Consumer<E> addConsumer(String groupId, String consumerId) {
        ConsumerGroup<E> reqGroup = consumerGroups.stream()
                                                  .filter(group -> group.getId().equals(groupId))
                                                  .findFirst()
                                                  .get();
        Consumer<E> newCons = reqGroup.addConsumer(consumerId);
        reqGroup.rebalance(this);
        return newCons;
    }

    /**
     * Returns whether the topic contains a consumer group with the specified id
     * @param groupId unique identifier for a consumer group
     * @return whether the group is in this topic
     */
    public boolean containsGroup(String groupId) {
        return consumerGroups.stream().anyMatch(group -> group.getId().equals(groupId));
    }

    /**
     * Returns whether the topic contains a consumer with a specified id
     * @param consumerId unique identifier for a consumer
     * @return whether this topic contains the specified consumer
     */
    public boolean containsConsumer(String consumerId) {
        return consumerGroups.stream().anyMatch(group -> group.containsConsumer(consumerId));
    }

    /**
     * Deletes a consumer from this topic
     * @param consumerId unique identifier for a consumer
     * @pre the consumerId corresponds to a valid consumer
     * @post the consumer no longer exists in this topic
     * @return void
     */
    public void deleteConsumer(String consumerId) {
        consumerGroups.stream().forEach(group -> {
            group.deleteConsumer(consumerId);
            group.rebalance(this);
        });
    }

    /**
     * Adds an event to the specified partition
     * @param producerId unique identifier for the producer
     * @param eventFileName identifier for the event to be produced
     * @param partitionId unique identifier for the partition
     * @pre the producerId corresponds to a valid producer. there exists a JSON file with
     * the name eventFileName. the partitionId corresponds to a valid partition
     * @post there is a new event with ID eventFileName in the specified partition
     * @return void
     */
    public void addEvent(String producerId, String eventFileName, String partitionId) {
        Partition<E> newPart =  partitions.stream()
                                          .filter(partition -> partition.getId().equals(partitionId))
                                          .findFirst()
                                          .get();
        newPart.addEvent(eventFileName, producerId);
    }

    /**
     * Consumes an event with the specified consumer from the specified partition
     * @param consumerId unique identifier for a consumer
     * @param partitionId unique identifier for a producer
     * @pre the consumerId and partitionId correspond to valid entities
     * @post the earliest event in the specified partition is consumed by the
     * specified consumer
     * @return void
     */
    public void consumeEvent(String consumerId, String partitionId) {
        ConsumerGroup<E> groupWithConsumer = consumerGroups.stream()
                                                           .filter(group -> group.containsConsumer(consumerId))
                                                           .findFirst()
                                                           .get();
        Consumer<E> consumer = groupWithConsumer.getConsumer(consumerId);
        Partition<E> partition = partitions.stream()
                                        .filter(part -> part.getId().equals(partitionId)).findFirst().get();
        partition.consumeEvent(consumer);
    }

    /**
     * Displays the details of the partitions and their events
     * @return void
     */
    public void displayPartitions() {
        partitions.stream().forEach(p -> {
            System.out.println("PartitionId: " + p.getId());
            System.out.println("This partition contains events: ");
            p.displayEvents();
        });
    }

    /**
     * Displays the details of the consumer group specified
     * @param groupId unique identifier for a consumer group
     * @pre the groupid corresponds to a valid consumer
     */
    public void displayGroup(String groupId) {
        consumerGroups.stream()
                    .filter(group -> group.getId().equals(groupId))
                    .forEach(group -> group.display());
    }

    /**
     * Sets the rebalancing strategy of a specified group to the specified strategy
     * @param groupId unqiue identifier of a consumer group
     * @param strategy strategy for rebalancing
     * @pre the groupId corresponds to a valid consumer group. the strategy is valid
     * @post the rebalancing strategy of the group is set to the specified strategy
     * @return void
     */
    public void setRebalancingStrategy(String groupId, String strategy) {
        consumerGroups.stream()
                    .filter(group -> group.getId().equals(groupId))
                    .forEach(group -> group.setRebalancingStrategy(strategy));
    }

    /**
     * Returns the consumer group with the specified id
     * @param consumerId unique identifier for a consumer group in this topic
     * @pre the consumerId corresponds to a valid consumer group in this topic
     * @return consumer group with the specified id
     */
    public ConsumerGroup<E> getGroupWithConsumer(String consumerId) {
        return consumerGroups.stream()
                             .filter(group -> group.containsConsumer(consumerId))
                             .findFirst()
                             .get();
    }
}
