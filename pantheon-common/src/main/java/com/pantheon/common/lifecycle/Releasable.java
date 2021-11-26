
package com.pantheon.common.lifecycle;


import java.io.Closeable;

/**
 * Specialization of {@link AutoCloseable} that may only throw an {@link }.
 */
public interface Releasable extends Closeable {

    @Override
    void close();

}
