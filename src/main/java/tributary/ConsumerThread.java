package tributary;

public class ConsumerThread extends Thread {
    private Tributary tributary;
    private String consumerId;
    private String partitionId;

    public ConsumerThread(Tributary tributary, String consumerId, String partitionId) {
        this.tributary = tributary;
        this.consumerId = consumerId;
        this.partitionId = partitionId;
    }

    public void run() {
        tributary.consumeEvent(consumerId, partitionId);
    }
}
