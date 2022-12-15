package tributary;

import java.util.ArrayList;
import java.util.List;

public class Tributary {
    private List<Topic<?>> topics;
    private List<Producer<?>> producers;

    /**
     * Constructor for Tributary system.
     * Returns a Tributary object that can then be built into an event-driven system.
     * @return Tributary object
     */
    public Tributary() {
        this.topics = new ArrayList<Topic<?>>();
        this.producers = new ArrayList<Producer<?>>();
    }

    /**
     * Method to create a new topic in the tributary system. The topic contains
     * partitions which hold messages to be processed/consumed.
     * @param <T> type of objects that the producer can produce
     * @param topicId unique ID to act as topic identifier
     * @param type type of objects that will be contained within the topic
     * @pre type is a valid type, topicId is unique
     * @post the system now contains a new topic with id topidId of the specified type
     * @return newly created topic
     */
    public <T> Topic<T> createTopic(String topicId, Class<T> type) {
        Topic<T> newTopic = new Topic<T>(topicId);
        topics.add(newTopic);
        System.out.println("You have successfully created a new topic with");
        System.out.println("ID: " + topicId);
        System.out.println("type: " + type.getSimpleName());
        return newTopic;
    }

    /**
     * Method to create a new partition in the tributary system.
     * This partition will be responsible for holding messages to be consumed
     * @param topicId unique ID to act as topic identifier
     * @param partitionId unique ID to act as partition identifier
     * @pre there does not exist a partition with id partitionId in the topic topicId
     * @post the system now contains a new partition with id partitionId in the topic with id topicId
     * @return newly created partition
     */
    public Partition<?> createPartition(String topicId, String partitionId) {
        Topic<?> topicReq = topics.stream()
                                .filter(topic -> topic.getId().equals(topicId))
                                .findFirst()
                                .get();
        Partition<?> newPart = topicReq.addPartition(partitionId);
        System.out.println("You have successfully created a partition with:");
        System.out.println("ID: " + partitionId);
        System.out.println("Topic: " + topicId);
        return newPart;
    }

    /**
     * Method to create a consumer group in the tributary system.
     * The consumer group contains consumers responsible for consuming messages
     * stored in partitions.
     * @param groupId
     * @param topicId unique ID to act as topic identifier
     * @param strategy String that will outline how the consumer group rebalances
     * upon addition/removal of new consumer
     * @pre there does not exist a consumer group with id groupId in the topic with id topicId
     * @post the system now contains a new consumer group with id groupId in the topic with id topicId
     * @return newly created consumer group
     */
    public ConsumerGroup<?> createConsumerGroup(String groupId, String topicId, String strategy) {
        Topic<?> topicReq = topics.stream()
                                .filter(topic -> topic.getId().equals(topicId))
                                .findFirst()
                                .get();
        ConsumerGroup<?> group = topicReq.addConsumerGroup(groupId, strategy);
        System.out.println("You have successfully created a consumer group with:");
        System.out.println("ID: " + groupId);
        System.out.println("Topic: " + topicId);
        System.out.println("Rebalancing Strategy: " + strategy);
        return group;
    }

    /**
     * Method to create a consumer in the tributary system
     * This consumer will be responsible for consuming messages from one or
     * more partitions in a topic.
     * After addition, the consumer group will rebalance and reallocate consumers
     * to different partitions
     * @param groupId unique ID to act as consumer group identifier
     * @param consumerId unique ID to act as consumer identifier
     * @pre there does not exist a consumer with id consumerId in the group with id groupId
     * @post the system now contains a new consumer with id consumerId in the group with id groupId
     * @return newly created consumer
     */
    public Consumer<?> createConsumer(String groupId, String consumerId) {
        Topic<?> topicWithGroup = topics.stream()
                            .filter(topic -> topic.containsGroup(groupId))
                            .findFirst().get();
        Consumer<?> newConsumer = topicWithGroup.addConsumer(groupId, consumerId);
        System.out.println("You have successfully created a consumer with:");
        System.out.println("ID: " + consumerId);
        System.out.println("Consumer Group: " + groupId);
        return newConsumer;
    }

    /**
     * Method to remove a consumer in the tributary system
     * After removal, the consumer group will rebalance and reallocate consumers
     * to different partitions
     * @param consumerId unique ID to act as consumer identifier
     * @pre There exists a consumer with id consumerId
     * @post There no longer exists a consumer with id consumerId
     * @return void
     */
    public void deleteConsumer(String consumerId) {
        Topic<?> topicReq = topics.stream().filter(topic -> topic.containsConsumer(consumerId)).findFirst().get();
        ConsumerGroup<?> group = topicReq.getGroupWithConsumer(consumerId);
        topicReq.deleteConsumer(consumerId);
        System.out.println("You have successfully deleted the consumer with:");
        System.out.println("ID: " + consumerId);
        group.display();
    }

    /**
     * Method to create a producer in the tributary system
     * This producer will be responsible for sending messages to the topics in
     * the system.
     * @param <T> type of objects that the producer can produce
     * @param producerId unique ID to act as producer identifier
     * @param type type of objects that the producer can produce
     * @param strategy String to indicate whether to send a message to a particular
     * partition by providing the relevant key or requesting random allocation
     * @pre there does not exist a producer with id producerId
     * @post the system now contains a new producer with id producerId
     * @return newly created producer
     */
    public <T> Producer<T> createProducer(String producerId, Class<T> type, String strategy) {
        Producer<T> newProd = new Producer<T>(producerId, strategy);
        producers.add(newProd);
        System.out.println("You have successfully created a producer with:");
        System.out.println("ID: " + producerId);
        System.out.println("Type: " + type.getSimpleName());
        System.out.println("Allocation: " + strategy);
        return newProd;
    }

    /**
     * Method to produce an event from a specific producer and send this event
     * to a specific topic.
     * It will retrieve the value of the event from a JSON file.
     * This method will see the producer randomly allocate the event into
     * any partition in the topic.
     * @param producerId unique ID to act as producer identifier
     * @param topicId unique ID to act as topic identifier
     * @param eventFileName name of JSON file containing event/message value
     * @post There exists a new event with id eventFileName
     * @return void
     */
    public void produceEvent(String producerId, String topicId, String eventFileName) {
        Topic<?> topicRequired = topics.stream()
                                       .filter(topic -> topic.getId().equals(topicId))
                                       .findFirst()
                                       .get();
        Producer<?> producerToAdd = producers.stream()
                 .filter(producer -> producer.getId().equals(producerId))
                 .findFirst()
                 .get();
        String partitionId = producerToAdd.getAllocation(0, topicRequired);
        topicRequired.addEvent(producerId, eventFileName, partitionId);
        System.out.println("You have successfully produced an event in partition " + partitionId + " with ");
        System.out.println("ID: " + eventFileName);
    }

    /**
     * Method to produce an event from a specific producer and send this event
     * to a specific topic.
     * It will retrieve the value of the event from a JSON file.
     * This method will see the producer allocate the event into a
     * partition with a specified ID
     * @param producerId unique ID to act as producer identifier
     * @param topicId unique ID to act as topic identifier
     * @param eventFileName name of JSON file containing event/message value
     * @param partition unique ID to act as partition identifier
     * @post There exists a new event with id eventFileName in partition with id partition
     * @return void
     */
    public void produceEvent(String producerId, String topicId, String eventFileName, String partition) {
        topics.stream()
        .filter(topic -> topic.getId().equals(topicId))
        .forEach(topic -> topic.addEvent(producerId, eventFileName, partition));
        System.out.println("You have successfully produced an event in partition " + partition + " with ");
        System.out.println("ID: " + eventFileName);
    }

    /**
     * Method to consume an event from a partition with a specified consumer.
     * The event will then be stored in the consumer in a list of consumed events.
     * @param consumerId unique ID to act as consumer identifier
     * @param partitionId unique ID to act as partition identifier
     * @pre There is at least one event in partition with id partitionId.
     * There is a consumer with id consumerId. The consumer is allocated the partition.
     * @post The partition with id partitionId has its earliest event consumed
     * and the consumer has this event added to its list of eventsConsumed.
     * @return void
     */
    public void consumeEvent(String consumerId, String partitionId) {
        topics.stream().forEach(topic -> topic.consumeEvent(consumerId, partitionId));
    }

    /**
     * Method to consume multiple events from a partition with a specified consumer.
     * These events will then be stored in the consumer in a list of consumed events.
     * @param consumerId unique ID to act as consumer identifier
     * @param partitionId unique ID to act as partition identifier
     * @param numEvents integer corresponding to how many events are to be consumed
     * @pre There is at least numEvents events in partition with id partitionId.
     * @post The partition with id partitionId has its earliest numEvents event(s) consumed
     * and the consumer has these event(s) added to its list of eventsConsumed.
     * @return void
     */
    public void consumeEvents(String consumerId, String partitionId, int numEvents) {
        while (numEvents > 0) {
            consumeEvent(consumerId, partitionId);
            numEvents--;
        }
    }

    /**
     * Method to display details of a specific topic.
     * Prints a visual display of the given topic,
     * including all partitions and all of the events currently in each partition.
     * @param topicId unique ID to act as topic identifier
     * @pre There exists a topic with id topicId
     * @return void
     */
    public void showTopic(String topicId) {
        Topic<?> topic = topics.stream().filter(t -> t.getId().equals(topicId)).findFirst().get();
        System.out.println("Now displaying topic with Id: " + topicId);
        System.out.println("This topic contains partitions: ");
        topic.displayPartitions();
    }

    /**
     * Method to display details of a specific consumer group.
     * Shows all consumers in the consumer group,
     * and which partitions each consumer is receiving events from.
     * @param groupId unique ID to act as consumer group identifier\
     * @pre There exists a consumer group with id groupId
     * @return void
     */
    public void showConsumerGroup(String groupId) {
        Topic<?> topicWithGroup = topics.stream()
                            .filter(topic -> topic.containsGroup(groupId))
                            .findFirst().get();
        topicWithGroup.displayGroup(groupId);
    }

    /**
     * Method to produce a series of events in parallel
     * @param args array of strings outlining ids/names of producers, topics and events to produce with/from.
     * The array will contain strings in the format [parallel produce producer topic event ...]
     * @pre The list of arguments fits the format [parallel produce producer topic event ...]
     * @post There exists multiple new events with ids corresponding to the event string in the list of arguments
     * @return void
     */
    public void parallelProduce(String[] args) {
        int arg1 = 2;
        int arg2 = 3;
        int arg3 = 4;
        int size = args.length - 1;
        while (size >= 4) {
            (new ProducerThread(this, args[arg1], args[arg2], args[arg3])).run();
            size -= 3;
            arg1 += 3;
            arg2 += 3;
            arg3 += 3;
        }
    }

    /**
     * Method to consume a series of events in parallel
     * @param args  array of strings outlining ids of consumers and partitions to consume with/from.
     * The array will contain strings in the format [parallel consume (consumer partition) ...]
     * @pre The list of arguments fits the format [parallel consume (consumer partition) ...].
     * @post The consumer consumes the events in the partitions according to the input
     * @return void
     */
    public void parallelConsume(String[] args) {
        int arg1 = 2;
        int arg2 = 3;
        int size = args.length - 1;
        while (size > 2) {
            (new ConsumerThread(this, args[arg1], args[arg2])).run();
            size -= 2;
            arg1 += 2;
            arg2 += 2;
        }
    }

    /**
     * Method to updatethe rebalancing method of consumer group to be one of Range or RoundRobin.
     * @param groupId unique ID to act as consumer group identifier
     * @param strategy String that will outline how the consumer group rebalances
     * upon addition/removal of new consumer
     * @pre strategy is either Range or RoundRobin
     * @post the consumer group with id groupId has new strategy of `strategy`.
     * @return void
     */
    public void setConsumerGroupRebalancing(String groupId, String strategy) {
        Topic<?> topicWithGroup = topics.stream()
                            .filter(topic -> topic.containsGroup(groupId))
                            .findFirst().get();
        topicWithGroup.setRebalancingStrategy(groupId, strategy);
        System.out.println(
            "You have successfully set consumer group with ID " + groupId + " to have strategy: " + strategy);
    }

    /**
     * Method to play back events for a given consumer from the offset.
     * @param consumerId unique ID to act as consumer identifier
     * @param partitionId unique ID to act as partition identifier
     * @param offset integer corresponding to offset of replay
     * @return void
     */
    public void playback(String consumerId, String partitionId, int offset) {
        //TODO
    }

}
