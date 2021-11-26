
package com.pantheon.common.lifecycle;



public interface LifecycleComponent extends Releasable {

    Lifecycle.State lifecycleState();

    void addLifecycleListener(LifecycleListener listener);

    void removeLifecycleListener(LifecycleListener listener);

    void start();

    void stop();
}
