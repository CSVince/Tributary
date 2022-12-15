package tributary;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Queue;

import org.json.JSONObject;

public class Partition<E> {
    private String id;
    private Queue<Event<E>> queue;

    /**
     * Constructor for partition
     * @param id unique identifier for partition
     * @return newly created partition
     */
    public Partition(String id) {
        this.id = id;
        this.queue = new LinkedList<>();
    }

    /**
     * Gets the id of the partition
     * @return String corresponding to partition ID
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the queue of events in the partition
     * @return Queue of events to be consumed
     */
    public Queue<Event<E>> getQueue() {
        return queue;
    }

    /**
     * Adds an event to this partition with the correct headers and value
     * @param eventFileName event ID and file name where event payload is stored
     * @param producerId unique ID of producer
     * @pre the eventFileName file exists and the producerId corresponds to a valid producer.
     * The producer type is the same as the partition type.
     * @post a new event with id eventFileName now exists in this partition
     * @return void
     */
    public synchronized void addEvent(String eventFileName, String producerId) {
        String eventsFolderPath;
        try {
            eventsFolderPath = Paths.get("").toAbsolutePath().toString()
                + "/src/main/java/events";
        } catch (Exception e) {
            throw new IllegalArgumentException();
        }
        Path eventsFolder = Paths.get(eventsFolderPath);
        String eventName = eventsFolder + "/" + eventFileName + ".json";

        InputStream is;
        try {
            is = new FileInputStream(eventName);
        } catch (Exception e) {
            throw new IllegalArgumentException(eventFileName + " could not be found!");
        }
        byte[] bytes = null;
        try {
             bytes = is.readAllBytes();
             is.close();
        } catch (Exception e) {
            throw new IllegalArgumentException(eventFileName + " could not be loaded!");
        }
        String contents = new String(bytes);
        JSONObject json = new JSONObject(contents);
        try {
            Event<E> newEvent = new Event<E>(
                eventFileName, json.get("value").getClass().getSimpleName(), producerId, json.get("value"));
            queue.add(newEvent);
        } catch (Exception e) {
        }
        return;
    }

    /**
     * Consumes an event from the partition queue
     * @param consumer the consumer consuming the event
     * @pre the consumer is a valid consumer
     * @post the event is removed from this partition queue. the event is added to the
     * list of consumed events in the consumer.
     * @return void
     */
    public synchronized void consumeEvent(Consumer<E> consumer) {
        Event<E> eventToBeConsumed;
        eventToBeConsumed = queue.remove();
        consumer.consumeEvent(eventToBeConsumed);
        System.out.println("Consumer " + consumer.getId() + " has successfully consumed event with: ");
        System.out.println("ID: " + eventToBeConsumed.getId());
        System.out.println("Value: " + eventToBeConsumed.getValue());
        System.out.println("The consumer now contains events with: ");
        consumer.displayEventsConsumed();
    }

    /**
     * Displays the events in the partition queue
     * @return void
     */
    public void displayEvents() {
        queue.stream().forEach(e -> {
            System.out.println("EventID: " + e.getId());
            System.out.println("Event message: " + e.getValue());
        });
    }
}
