package tributary;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TributaryTest {
    // Unit tests and integration tests
    @Test
    @DisplayName("Test create topic")
    public void testCreateTopic() {
        Tributary trb = new Tributary();
        Topic top = trb.createTopic("firstTopic", String.class);
    }

    @Test
    @DisplayName("Test create partition")
    public void testCreatePartition() {
        Tributary trb = new Tributary();
        Topic<String> top = trb.createTopic("firstTopic", String.class);
        assertEquals(top.getPartitions().size(), 0);
        trb.createPartition("firstTopic", "firstPartition");
        assertEquals(top.getPartitions().size(), 1);
    }

    @Test
    @DisplayName("Test create consumer group")
    public void testCreateConsumerGroup() {
        Tributary trb = new Tributary();
        Topic<String> top = trb.createTopic("firstTopic", String.class);
        assertEquals(top.getConsumerGroups().size(), 0);
        trb.createConsumerGroup("firstGroup", "firstTopic", "Range");
        assertEquals(top.getConsumerGroups().size(), 1);
        trb.createConsumerGroup("secondGroup", "firstTopic", "RoundRobin");
        assertEquals(top.getConsumerGroups().size(), 2);
    }

    @Test
    @DisplayName("Test create consumer")
    public void testCreateConsumer() {
        Tributary trb = new Tributary();
        trb.createTopic("firstTopic", String.class);
        ConsumerGroup<?> cons = trb.createConsumerGroup("firstGroup", "firstTopic", "Range");
        trb.createConsumerGroup("secondGroup", "firstTopic", "RoundRobin");
        assertEquals(cons.getConsumers().size(), 0);
        trb.createConsumer("firstGroup", "firstConsumer");
        assertEquals(cons.getConsumers().size(), 1);
    }

    @Test
    @DisplayName("Test delete consumer")
    public void testDeleteConsumer() {
        Tributary trb = new Tributary();
        trb.createTopic("firstTopic", String.class);
        ConsumerGroup<?> cons = trb.createConsumerGroup("firstGroup", "firstTopic", "Range");
        trb.createConsumerGroup("secondGroup", "firstTopic", "RoundRobin");
        assertEquals(cons.getConsumers().size(), 0);
        trb.createConsumer("firstGroup", "firstConsumer");
        assertEquals(cons.getConsumers().size(), 1);
        trb.deleteConsumer("firstConsumer");
        assertEquals(cons.getConsumers().size(), 0);
    }

    @Test
    @DisplayName("Test create producer")
    public void testCreateProducer() {
        Tributary trb = new Tributary();
        trb.createTopic("firstTopic", String.class);
        trb.createPartition("firstTopic", "firstPartition");
        trb.createProducer("producerOne", String.class, "Random");
        trb.createTopic("secondTopic", Integer.class);
        trb.createPartition("secondTopic", "secondPartition");
        trb.createProducer("producerTwo", Integer.class, "Manual");
        // If user passes in something other than manual and random,
        // it will default to Random
        trb.createProducer("producerThree", Integer.class, "");
    }

    @Test
    @DisplayName("Test produce event")
    public void testProduceEvent() {
        Tributary trb = new Tributary();
        trb.createTopic("firstTopic", String.class);
        trb.createPartition("firstTopic", "firstPartition");
        trb.createProducer("producerOne", String.class, "Random");
        trb.produceEvent("producerOne", "firstTopic", "sampleEventString");
        trb.createTopic("secondTopic", Integer.class);
        trb.createPartition("secondTopic", "secondPartition");
        trb.createProducer("producerTwo", Integer.class, "Manual");
        assertDoesNotThrow(() -> trb.produceEvent("producerTwo", "secondTopic",
        "sampleEventInteger", "secondPartition"));
    }

    @Test
    @DisplayName("Test consume Event")
    public void testConsumeEvent() {
        Tributary trb = new Tributary();
        trb.createTopic("firstTopic", String.class);
        Partition<?> part = trb.createPartition("firstTopic", "firstPartition");
        assertEquals(part.getQueue().size(), 0);
        trb.createProducer("producerOne", String.class, "Random");
        trb.produceEvent("producerOne", "firstTopic", "sampleEventString");
        assertEquals(part.getQueue().size(), 1);
        trb.createConsumerGroup("firstGroup", "firstTopic", "Range");
        Consumer<?> cons = trb.createConsumer("firstGroup", "firstConsumer");
        assertEquals(cons.getEventsConsumed().size(), 0);
        trb.consumeEvent("firstConsumer", "firstPartition");
        assertEquals(cons.getEventsConsumed().size(), 1);
    }

    @Test
    @DisplayName("Test consume events")
    public void testConsumeEvents() {
        Tributary trb = new Tributary();
        trb.createTopic("firstTopic", String.class);
        trb.createPartition("firstTopic", "firstPartition");
        trb.createProducer("producerOne", String.class, "Random");
        // Create two events
        trb.produceEvent("producerOne", "firstTopic", "sampleEventString");
        trb.produceEvent("producerOne", "firstTopic", "sampleEventString2");
        trb.createConsumerGroup("firstGroup", "firstTopic", "Range");
        Consumer<?> cons = trb.createConsumer("firstGroup", "firstConsumer");
        assertEquals(cons.getEventsConsumed().size(), 0);
        trb.consumeEvents("firstConsumer", "firstPartition", 2);
        assertEquals(cons.getEventsConsumed().size(), 2);
    }

    @Test
    @DisplayName("Test show topic")
    public void testShowTopic() {
        Tributary trb = new Tributary();
        trb.createTopic("firstTopic", String.class);
        trb.createPartition("firstTopic", "firstPartition");
        trb.createProducer("producerOne", String.class, "Random");
        trb.produceEvent("producerOne", "firstTopic", "sampleEventString");
        trb.produceEvent("producerOne", "firstTopic", "sampleEventString2");
        assertDoesNotThrow(() -> trb.showTopic("firstTopic"));
    }

    @Test
    @DisplayName("Test show consumer group")
    public void testShowConsumerGroup() {
        Tributary trb = new Tributary();
        trb.createTopic("firstTopic", String.class);
        trb.createPartition("firstTopic", "firstPartition");
        trb.createPartition("firstTopic", "secondPartition");
        trb.createPartition("firstTopic", "thirdPartition");
        trb.createProducer("producerOne", String.class, "Manual");
        // Create two events
        trb.produceEvent("producerOne", "firstTopic", "sampleEventString", "firstPartition");
        trb.produceEvent("producerOne", "firstTopic", "sampleEventString2", "firstPartition");
        trb.createConsumerGroup("firstGroup", "firstTopic", "Range");
        trb.createConsumer("firstGroup", "firstConsumer");
        trb.createConsumer("firstGroup", "secondConsumer");
        trb.createConsumer("firstGroup", "thirdConsumer");
        trb.consumeEvents("firstConsumer", "firstPartition", 2);
        assertDoesNotThrow(() -> trb.showConsumerGroup("firstGroup"));
    }

    @Test
    @DisplayName("Test parallel produce")
    public void testParallelProduce() {
        Tributary trb = new Tributary();
        trb.createTopic("firstTopic", String.class);
        Partition<?> part = trb.createPartition("firstTopic", "firstPartition");
        trb.createProducer("producerOne", String.class, "Random");
        trb.createTopic("secondTopic", Integer.class);
        trb.createPartition("secondTopic", "secondPartition");
        trb.createProducer("producerTwo", Integer.class, "Random");
        String[] args = new String[8];
        args[0] = "parallel";
        args[1] = "produce";
        args[2] = "producerTwo";
        args[3] = "firstTopic";
        args[4] = "sampleEventInteger";
        args[5] = "producerOne";
        args[6] = "firstTopic";
        args[7] = "sampleEventString";
        trb.parallelProduce((args));
        assertEquals(part.getQueue().size(), 2);
    }

    @Test
    @DisplayName("Test parallel consume")
    public void testParallelConsume() {
        Tributary trb = new Tributary();
        trb.createTopic("firstTopic", String.class);
        Partition<?> part = trb.createPartition("firstTopic", "firstPartition");
        trb.createProducer("producerOne", String.class, "Random");
        // Create two events
        trb.produceEvent("producerOne", "firstTopic", "sampleEventString");
        trb.produceEvent("producerOne", "firstTopic", "sampleEventString2");
        trb.createConsumerGroup("firstGroup", "firstTopic", "Range");
        Consumer<?> cons = trb.createConsumer("firstGroup", "firstConsumer");
        assertEquals(cons.getEventsConsumed().size(), 0);
        assertEquals(part.getQueue().size(), 2);
        String[] args = new String[6];
        args[0] = "parallel";
        args[1] = "consume";
        args[2] = "firstConsumer";
        args[3] = "firstPartition";
        args[4] = "firstConsumer";
        args[5] = "firstPartition";
        trb.parallelConsume(args);
        assertEquals(part.getQueue().size(), 0);
        assertEquals(cons.getEventsConsumed().size(), 2);
    }

    @Test
    @DisplayName("Test set consumer group rebalancing")
    public void testSetRebalancing() {
        Tributary trb = new Tributary();
        trb.createTopic("firstTopic", String.class);
        trb.createPartition("firstTopic", "firstPartition");
        trb.createConsumerGroup("firstGroup", "firstTopic", "Range");
        trb.createConsumerGroup("secondGroup", "firstTopic", "RoundRobin");
        // If the user passes in something other than RoundRobin or Range, it
        // will default to range
        trb.createConsumerGroup("thirdGroup", "firstTopic", "");
        trb.setConsumerGroupRebalancing("firstGroup", "RoundRobin");
        trb.setConsumerGroupRebalancing("secondGroup", "Range");
        trb.createConsumer("firstGroup", "consumer1");
        trb.createConsumer("secondGroup", "consumer2");
        // If the user passes in something other than RoundRobin or Range, it
        // will default to range
        trb.setConsumerGroupRebalancing("secondGroup", "");
    }

    // System tests
    @Test
    @DisplayName("Typical Message Lifespan")
    public void testMessageLifespan() {
        Tributary trb = new Tributary();
        trb.createTopic("user profiles", String.class);
        Partition<?> part = trb.createPartition("user profiles", "partition");
        trb.createConsumerGroup("consumer group", "user profiles", "Range");
        Consumer<?> cons = trb.createConsumer("consumer group", "first consumer");
        trb.createProducer("producer", String.class, "Random");
        trb.produceEvent("producer", "user profiles", "sampleEventString");
        assertEquals(cons.getEventsConsumed().size(), 0);
        assertEquals(part.getQueue().size(), 1);
        trb.showTopic("user profiles");
        trb.consumeEvent("first consumer", "partition");
        assertEquals(cons.getEventsConsumed().size(), 1);
        assertEquals(part.getQueue().size(), 0);
        trb.showTopic("user profiles");
        trb.showConsumerGroup("consumer group");
    }

    @Test
    @DisplayName("Test system rebalancing")
    public void testSystemRebalancing() {
        Tributary trb = new Tributary();
        trb.createTopic("user profiles", String.class);
        Partition<?> part1 = trb.createPartition("user profiles", "partition1");
        Partition<?> part2 = trb.createPartition("user profiles", "partition2");
        Partition<?> part3 = trb.createPartition("user profiles", "partition3");
        trb.createConsumerGroup("consumer group", "user profiles", "Range");
        Consumer<?> cons1 = trb.createConsumer("consumer group", "first consumer");
        assertEquals(cons1.getPartitions().size(), 3);
        Consumer<?> cons2 = trb.createConsumer("consumer group", "second consumer");
        assertEquals(cons1.getPartitions().size(), 2);
        assertEquals(cons2.getPartitions().size(), 1);
        Consumer<?> cons3 = trb.createConsumer("consumer group", "third consumer");
        assertEquals(cons1.getPartitions().size(), 1);
        assertEquals(cons2.getPartitions().size(), 1);
        assertEquals(cons3.getPartitions().size(), 1);
    }
}
