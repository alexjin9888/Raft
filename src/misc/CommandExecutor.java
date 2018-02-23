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

// TODO: stress in class comments that commands are executed in order
public class CommandExecutor {
    /**
     * Single thread manager that will execute the commands for us in order.
     */
    private ExecutorService singleThreadService;

    public CommandExecutor() {
        singleThreadService = Executors.newSingleThreadExecutor();
    }

    // TODO: Document that this class throws `CommandExecutorExceptions` in
    // threads that it controls.
    public CommandExecutor(UncaughtExceptionHandler ueh) {
        singleThreadService = Executors.newSingleThreadExecutor(new ThreadFactory() {
            public Thread newThread(Runnable r) {
                final Thread t = new Thread(r);
                t.setUncaughtExceptionHandler(ueh);
                return t;
            }
        });
    }

    // ERROR2DO: Mention in comments: `ueh` can be used to handle any uncaught
    // exceptions that may be thrown as a result of calling a callback after
    // executing a command.
    // It can also be used to deal with any uncaught CommandExecution exceptions
    // that may be thrown while trying to execute a bash command.
    // TODO: Mention that the callback will be called with the stdout/stderr
    // output that is generated from trying to execute that command.
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
