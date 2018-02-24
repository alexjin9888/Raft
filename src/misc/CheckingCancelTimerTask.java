package misc;

import java.util.TimerTask;

/**
 * A TimerTask subclass that allows you to check whether a TimerTask has
 * been cancelled.
 */
public abstract class CheckingCancelTimerTask extends TimerTask {
    /**
     * True iff the cancel method has been previously called.
     */
    public boolean isCancelled;
    
    public CheckingCancelTimerTask() {
        super();
        isCancelled = false;
    }
    
    /**
     * Wrapper method around java.util.TimerTask#cancel() to set a cancelled
     * flag.
     * @see java.util.TimerTask#cancel()
     */
    public boolean cancel() {
        isCancelled = true;
        return super.cancel();
    }
    
}
