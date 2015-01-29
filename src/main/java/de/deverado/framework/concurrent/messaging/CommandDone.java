package de.deverado.framework.concurrent.messaging;

import com.google.common.base.MoreObjects;

/**
 * Create an implementation in your protocol.
 * @param <T> the command which this done belongs to
 */
public abstract class CommandDone<T> {

    private long elapsed = -1;

    private T command;

    private String msg;

    private String err;

    private String exc;

    public long getElapsed() {
        return elapsed;
    }

    public void setElapsed(long elapsed) {
        this.elapsed = elapsed;
    }

    public T getCommand() {
        return command;
    }

    public void setCommand(T command) {
        this.command = command;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getErr() {
        return err;
    }

    public void setErr(String err) {
        this.err = err;
    }

    public String getExc() {
        return exc;
    }

    /**
     * This should be toString of Throwable.
     * 
     * @param exc set
     */
    public void setExc(String exc) {
        this.exc = exc;
    }

    /**
     * Get only.
     * 
     * @param ign ignored
     */
    public void setErroneous(boolean ign) {

    }

    public boolean isErroneous() {
        return err != null || exc != null;
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this)
                .add("elapsed", getElapsed()).add("msg", getMsg());
        if (isErroneous()) {
            helper.add("err", getErr()).add("exc", exc.toString());
        }
        return helper.toString();
    }
}
