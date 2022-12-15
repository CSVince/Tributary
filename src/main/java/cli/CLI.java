package cli;

import java.io.Console;

import tributary.Tributary;

public class CLI {

    public static void main(String[] args) {
        Tributary ctrl = new Tributary();
        Console console = System.console();
        if (console == null) {
            return;
        }
        String str;
        System.out.println("Welcome! Enter commands to build your event-driven system. "
        + "Press q at any time to quit.");
        while ((str = console.readLine("Command: ")) != null) {
            System.out.println("");
            CommandFactory.execute((str.split(" ")), ctrl);
            System.out.println("");
            if ((str.split(" "))[0].equals("q")) {
                return;
            }
        }
    }
}
