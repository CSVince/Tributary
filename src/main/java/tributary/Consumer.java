package tributary;

import java.util.ArrayList;
import java.util.List;

public class Consumer<E> {
    private String id;
    private List<Event<E>> eventsConsumed;
    private List<Partition<E>> partitions = new ArrayList<>();

    /**
     * Constructor for consumer
     * @param id Unique identifier for Consumer
     * @return returns a new consumer entity with Id id.
     */
    public Consumer(String id) {
        this.id = id;
        this.eventsConsumed = new ArrayList<>();
    }

    /**
     * Get the consumer ID
     * @return String for consumer ID
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the ID of the partition this consumer is allocated to
     * @return String corresponding to partition Id
     */
    public List<Partition<E>> getPartitions() {
        return partitions;
    }

    /**
     * Gets the list of consumed events
     * @return list of consumed events
     */
    public List<Event<E>> getEventsConsumed() {
        return eventsConsumed;
    }

    /**
     * Consumes an event and adds it to the list of consumed events
     * @param event event to be consumed
     * @pre event is a valid event
     * @post the event is added to the list of consumed events
     * @return void
     */
    public synchronized void consumeEvent(Event<E> event) {
        eventsConsumed.add(event);
    }

    /**
     * Displays all the IDs of the consumed events
     * @return void
     */
    public void displayEventsConsumed() {
        eventsConsumed.stream()
                      .forEach(e -> {
                        System.out.println("EventID: " + e.getId());
                      });
    }

    /**
     * Adds a partition to be allocated to this consumer
     * @param partition partition to be allocated
     * @pre the partition is a valid partition
     * @post the partition is added to the list of partitions allocated to this consumer
     * @return void
     */
    public synchronized void addPartition(Partition<E> partition) {
        partitions.add(partition);
    }

    /**
     * Displays all the IDs of the consumers allocated to this consumer
     * @return void
     */
    public void displayPartitions() {
        partitions.stream()
                  .forEach(p -> System.out.println(p.getId() + " "));
    }

    /**
     * Reset the partition allocations of this consumer.
     * @post this consumer has no partitions allocated to it
     * @return void
     */
    public void resetAllocation() {
        this.partitions = new ArrayList<>();
    }

}
