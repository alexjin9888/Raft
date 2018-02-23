package misc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * This class schedules bash commands to be executed in the order that they
 * are queued up with the CommandExecutor instance.
 */
public class CommandExecutor {
    /**
     * Manager for single thread that will execute the commands for us.
     */
    private ExecutorService singleThreadService;

    public CommandExecutor() {
        singleThreadService = Executors.newSingleThreadExecutor();
    }

    /**
     * @param ueh Function for handling uncaught exceptions that may be thrown
     *            while executing a command (CommandExecutionException) or in
     *            the caller's processing of the command result (i.e., any
     *            unchecked exception that the callback code might throw).
     */
    public CommandExecutor(UncaughtExceptionHandler ueh) {
        singleThreadService = Executors.newSingleThreadExecutor(
                new ThreadFactory() {
            public Thread newThread(Runnable r) {
                final Thread t = new Thread(r);
                t.setUncaughtExceptionHandler(ueh);
                return t;
            }
        });
    }

    /**
     * Schedules a Bash command for execution, and calls the passed-in callback
     * with the command's output after the command has been executed.
     * @param command Bash command to be executed.
     * @param handleCommandResultCb Callback that is called with the
     * stdout/stderr output from executing the Bash command.
     */
    public synchronized void execute(String command, Consumer<String> handleCommandResultCb) {
        singleThreadService.execute(() -> {
            String result = "";

            try {
                Process p = new ProcessBuilder("bash", "-c", command)
                        .redirectErrorStream(true)
                        .start();

                p.waitFor();
                try (InputStream is = p.getInputStream();
                        Scanner s = new Scanner(is)) {;
                    s.useDelimiter("\\A");
                    result = s.hasNext() ? s.next() : "";
                }
            } catch (IOException e) {
                throw new CommandExecutorException("Client command execution"
                        + " failed. Received error: " + e);
                // handleCommandResultCb.accept(new CommandExecutorException("TODO"), null);
            } catch (InterruptedException e) {
                throw new CommandExecutorException("Client command execution"
                        + " interrupted. Received error: " + e);
            }

            handleCommandResultCb.accept(result);
        });
    }

}
