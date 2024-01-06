package com.core.infrastructure.collections;

import java.util.NoSuchElementException;

/**
 * A linear collection that supports element insertion and removal at both ends.
 * The name <i>deque</i> is short for "double ended queue" and is usually pronounced "deck".
 *
 * <p>This interface defines methods to access the elements at both ends of the deque.
 * Methods are provided to insert, remove, and examine the element.
 * It is a simplified version of the Java {@link java.util.Deque} interface used for internal infrastructure components.
 *
 * @param <E> the type of elements held in this deque
 */
public interface Deque<E> {

    /**
     * Returns true if the list contains the specified element.
     *
     * @param item the element
     * @return true if the list contains the specified element
     */
    boolean contains(E item);

    /**
     * Inserts the specified element at the end of this list.
     *
     * @param item the element to add
     */
    void addFirst(E item);

    /**
     * Inserts the specified element at the end of this list.
     *
     * @param item the element to add
     */
    void addLast(E item);

    /**
     * Inserts the specified element at the end of this list.
     *
     * @param item the element to add
     * @implSpec the default implementation invokes {@code addLast} on the item
     */
    default void add(E item) {
        addLast(item);
    }

    /**
     * Retrieves and removes the head (first element) of this list.
     *
     * @return the head of this list
     * @throws NoSuchElementException if this list is empty
     */
    E removeFirst();

    /**
     * Retrieves and removes the head (first element) of this list.
     *
     * @return the head of this list
     * @throws NoSuchElementException if this list is empty
     */
    E removeLast();

    /**
     * Retrieves, but does not remove, the head (first element) of this list.
     *
     * @return the head of this list, or {@code null} if this list is empty
     */
    E getFirst();

    /**
     * Retrieves, but does not remove, the tail (last element) of this list.
     *
     * @return the fail of this list, or {@code null} if this list is empty
     */
    E getLast();

    /**
     * Returns the number of elements in the list.
     *
     * @return the number of elements in the list
     */
    int size();

    /**
     * Returns true if this list contains no elements.
     *
     * @return true if this list contains no elements
     */
    boolean isEmpty();
}
