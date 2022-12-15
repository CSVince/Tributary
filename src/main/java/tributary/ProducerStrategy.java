package tributary;

public interface ProducerStrategy {
    public int allocateMessage(int key, Topic<?> topic);
}
