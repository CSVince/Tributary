package tributary;

import java.time.LocalDateTime;

public class Event<E> {
    private LocalDateTime creationDate;
    private String id;
    private String payloadType;
    private String source;
    private Object value;

    /**
     * Constructor for a new message/event
     * @param id unique identifier for event
     * @param payloadType type of payload
     * @param source id of producer which sent the message
     * @param value the payload of the message
     */
    public Event(String id, String payloadType, String source, Object value) {
        this.id = id;
        this.payloadType = payloadType;
        this.source = source;
        this.value = value;
        this.creationDate = LocalDateTime.now();
    }

    /**
     * Gets the id of the event
     * @return String corresponding to event ID
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the value of the event
     * @return Value of the event
     */
    public Object getValue() {
        return value;
    }

    /**
     * Gets the date when the event was created
     * @return date when the event was created
     */
    public LocalDateTime getCreationDate() {
        return creationDate;
    }

    /**
     * Gets the id for the producer which produced the event
     * @return id of producer
     */
    public String getSource() {
        return source;
    }

    /**
     * Gets the type of the payload
     * @return type of payload
     */
    public String getPayloadType() {
        return payloadType;
    }
}
