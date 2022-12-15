package tributary;

public class ManualProducer implements ProducerStrategy {

    public ManualProducer() {
    }

    public int allocateMessage(int key, Topic<?> topic) {
        return key;
    }
}
