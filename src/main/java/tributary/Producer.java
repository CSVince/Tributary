package tributary;

public class Producer<E> {
    private String id;
    private ProducerStrategy producingStrategy;

    /**
     * Constructor for producer
     * @param id identifier for producer
     * @param strategy producing/allocation strategy for producer
     * @return new producer
     */
    public Producer(String id, String strategy) {
        this.id = id;
        switch (strategy) {
            case "Random":
                this.producingStrategy = new RandomProducer();
                break;
            case "Manual":
                this.producingStrategy = new ManualProducer();
                break;
            default:
                this.producingStrategy = new RandomProducer();
                break;
        }
    }

    /**
     * Gets the id of the producer
     * @return String corresponding to producer ID
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the id of the partition that an event will be produced to
     * @param key specified key if the producer is a manual producer
     * @param topic topic where the event will be produced
     * @pre the topic is valid and the key is a valid integer (if the producer is a manual producer)
     * @return partition Id where event will be produced
     */
    public String getAllocation(int key, Topic<?> topic) {
        return topic.getPartitions().get(producingStrategy.allocateMessage(key, topic)).getId();
    }

}
