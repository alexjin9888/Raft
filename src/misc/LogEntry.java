package misc;
/*
 * Each instance of LogEntry class corresponds to a log.
 * Proj2: figure out what to do with command
 */
public class LogEntry {
    public String command;
    public int term;

    public LogEntry(String command, int term) {
        super();
        this.command = command;
        this.term = term;
    }

    @Override
    public String toString() {
        return "LogEntry [command=" + command + ", term=" + term + "]";
    }
}
