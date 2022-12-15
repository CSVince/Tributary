package tributary;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import cli.CLI;

import static org.junit.jupiter.api.Assertions.*;

public class CLITest {
    @Test
    @DisplayName("Test main")
    public void testMain() {
        CLI.main("create topic hello String".split(" "));
    }
}
