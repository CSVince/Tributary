package tributary;

public class RandomProducer implements ProducerStrategy {

    public RandomProducer() {
    }

    public int allocateMessage(int key, Topic<?> topic) {
        int size = topic.getPartitions().size();
        return (int) (Math.random() * size);
    }

}
