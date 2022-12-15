package cli;

import tributary.Tributary;

public class CommandFactory {
    public static void createEntity(String[] args, Tributary ctrl) {
        switch (args[1]) {
            case "topic":
                if (args[3].equals("String")) {
                    ctrl.createTopic(args[2], String.class);
                } else {
                    ctrl.createTopic(args[2], Integer.class);
                }
                return;
            case "partition":
                ctrl.createPartition(args[2], args[3]);
                return;
            case "consumer":
                if (args[2].equals("group")) {
                    ctrl.createConsumerGroup(args[3], args[4], args[5]);
                } else {
                    ctrl.createConsumer(args[2], args[3]);
                }
                return;
            case "producer":
                if (args[3].equals("String")) {
                    ctrl.createProducer(args[2], String.class, args[4]);
                } else {
                    ctrl.createProducer(args[2], Integer.class, args[4]);
                }
                return;
            default:
                return;
        }
    }

    public static void execute(String[] args, Tributary ctrl) {
        switch (args[0]) {
            case "create":
                createEntity(args, ctrl);
                return;
            case "delete":
                ctrl.deleteConsumer(args[2]);
                return;
            case "produce":
                if (args.length == 5) {
                    ctrl.produceEvent(args[2], args[3], args[4]);
                } else {
                    ctrl.produceEvent(args[2], args[3], args[4], args[5]);
                }
                return;
            case "consume":
                if (args.length == 4) {
                    ctrl.consumeEvent(args[2], args[3]);
                } else {
                    ctrl.consumeEvents(args[2], args[3], Integer.parseInt(args[4]));
                }
                return;
            case "show":
                if (args[1].equals("topic")) {
                    ctrl.showTopic(args[2]);
                } else {
                    ctrl.showConsumerGroup(args[3]);
                }
                return;
            case "parallel":
                if (args[1].equals("produce")) {
                    ctrl.parallelProduce(args);
                } else {
                    ctrl.parallelConsume(args);
                }
                return;
            case "set":
                ctrl.setConsumerGroupRebalancing(args[4], args[5]);
                return;
            default:
                return;
        }
    }
}
