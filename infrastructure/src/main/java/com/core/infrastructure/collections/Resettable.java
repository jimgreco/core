package com.core.infrastructure.collections;

/**
 * Pooled objects have state that is recycled between uses of an object.
 * A {@code Resettable} object has a single method, {@link #reset()} to reset the state when the object is returned to
 * the object pool.
 */
public interface Resettable {

    /**
     * Invoked when the object is returned to the object pool to return the object to a know state.
     */
    void reset();
}
