package com.core.infrastructure.collections;

import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * An object pool is a reusable collection of objects.
 *
 * <p>To borrow an item from the object pool, invoke the {@link #borrowObject()} method.
 * To return an object from the pool, invoke the {@link #returnObject(Object)} method with the borrowed object.
 * The pool will expand in size if all items in the pool are exhausted.
 *
 * <p>No validation is done to validate that objects returned to the pool were originally borrowed from the pool or have
 * not been returned already.
 *
 * @param <T> the object type
 */
public class ObjectPool<T> implements Encodable {

    private static final int INITIAL_SIZE = 10;

    private final Supplier<T> objectSupplier;
    private T[] items;
    private int outstanding;

    /**
     * Creates a pool of objects, initially of size 10, that are created from the specified supplier.
     *
     * @param objectSupplier a supplier of the pool's objects
     */
    public ObjectPool(Supplier<T> objectSupplier) {
        this(objectSupplier, INITIAL_SIZE);
    }

    /**
     * Creates a pool of objects, initially of the size specified, that are created from the specified supplier.
     *
     * @param objectSupplier a supplier of the pool's objects
     * @param initialSize the initial number of objects allocated
     */
    @SuppressWarnings("unchecked")
    public ObjectPool(Supplier<T> objectSupplier, int initialSize) {
        this.objectSupplier = Objects.requireNonNull(objectSupplier);
        var firstItem = objectSupplier.get();
        items = (T[]) Array.newInstance(firstItem.getClass(), Math.max(1, initialSize));
        items[0] = firstItem;
        for (var i = 1; i < items.length; i++) {
            items[i] = objectSupplier.get();
        }
    }

    /**
     * Retrieves an object from the pool.
     * The pool will increase its size if the number of objects remaining in the pool is zeor.
     *
     * @return an object from the pool
     */
    public T borrowObject() {
        if (outstanding == items.length) {
            var size = items.length;
            items = Arrays.copyOf(items, 2 * items.length);
            for (var i = size; i < items.length; i++) {
                items[i] = objectSupplier.get();
            }
        }
        return items[outstanding++];
    }

    /**
     * Returns an object to the pool.
     *
     * @param element the element to return to the pool
     * @throws IllegalStateException if the pool does not have any outstanding items
     */
    public void returnObject(T element) {
        if (outstanding == 0) {
            throw new IllegalStateException("returned too many items to pool");
        }
        if (element instanceof Resettable obj) {
            obj.reset();
        }
        items[--outstanding] = element;
    }

    /**
     * Returns the total number of objects that have been borrowed from the pool.
     *
     * @return the total number of objects that have been borrowed from the pool
     */
    @Command(readOnly = true)
    public int getOutstanding() {
        return outstanding;
    }

    /**
     * Returns the total number of objects managed by the pool.
     *
     * @return the total number of objects managed by the pool
     */
    @Command(readOnly = true)
    public int getSize() {
        return items.length;
    }

    /**
     * Returns the number of objects remaining in the pool.
     *
     * @return the number of objects remaining in the pool
     */
    @Command(readOnly = true)
    public int getRemaining() {
        return items.length - outstanding;
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    @Command(path = "status", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("size").number(getSize())
                .string("outstanding").number(getOutstanding())
                .string("remaining").number(getRemaining())
                .closeMap();
    }
}
