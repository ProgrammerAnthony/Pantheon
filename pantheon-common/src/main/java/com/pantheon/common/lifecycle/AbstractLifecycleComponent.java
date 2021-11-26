
package com.pantheon.common.lifecycle;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class AbstractLifecycleComponent implements LifecycleComponent {

    protected final Lifecycle lifecycle = new Lifecycle();

    private final List<LifecycleListener> listeners = new CopyOnWriteArrayList<>();

    protected AbstractLifecycleComponent() {}

    @Override
    public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void start() {
        synchronized (lifecycle) {
            if (!lifecycle.canMoveToStarted()) {
                return;
            }
            for (LifecycleListener listener : listeners) {
                listener.beforeStart();
            }
            doStart();
            lifecycle.moveToStarted();
            for (LifecycleListener listener : listeners) {
                listener.afterStart();
            }
        }
    }

    protected abstract void doStart();

    @Override
    public void stop() {
        synchronized (lifecycle) {
            if (!lifecycle.canMoveToStopped()) {
                return;
            }
            for (LifecycleListener listener : listeners) {
                listener.beforeStop();
            }
            lifecycle.moveToStopped();
            doStop();
            for (LifecycleListener listener : listeners) {
                listener.afterStop();
            }
        }
    }

    protected abstract void doStop();

    @Override
    public void close() {
        synchronized (lifecycle) {
            if (lifecycle.started()) {
                stop();
            }
            if (!lifecycle.canMoveToClosed()) {
                return;
            }
            for (LifecycleListener listener : listeners) {
                listener.beforeClose();
            }
            lifecycle.moveToClosed();
            try {
                doClose();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                for (LifecycleListener listener : listeners) {
                    listener.afterClose();
                }
            }
        }
    }

    protected abstract void doClose() throws IOException;
}
