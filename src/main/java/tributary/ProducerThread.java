package tributary;

public class ProducerThread extends Thread {
    private Tributary tributary;
    private String producerId;
    private String topicId;
    private String eventFileName;
    private String partition = null;

    public ProducerThread(Tributary tributary, String producerId, String topicId, String eventFileName) {
        this.tributary = tributary;
        this.producerId = producerId;
        this.topicId = topicId;
        this.eventFileName = eventFileName;
    }

    public ProducerThread(Tributary tributary, String producerId, String topicId,
        String eventFileName, String partition) {
        this.tributary = tributary;
        this.producerId = producerId;
        this.topicId = topicId;
        this.eventFileName = eventFileName;
        this.partition = partition;
    }

    public void run() {
        if (partition == null) {
            tributary.produceEvent(producerId, topicId, eventFileName);
        } else {
            tributary.produceEvent(producerId, topicId, eventFileName, partition);
        }
    }

}
