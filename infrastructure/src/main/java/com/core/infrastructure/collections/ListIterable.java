package com.core.infrastructure.collections;

import java.util.ListIterator;

/**
 * An iterable for lists that provides a {@code ListIterator} which allows the programmer to traverse the list in either
 * direction, retrieve the first and last elements of the list, and retrieve the size of the list.
 *
 * @param <E> the list element type
 */
public interface ListIterable<E> extends Iterable<E> {

    /**
     * Returns a list iterator over the elements in this list (in proper sequence).
     *
     * @return a list iterator over the elements in this list (in proper sequence)
     */
    ListIterator<E> listIterator();

    /**
     * Returns the number of elements in this list.
     * If this list contains more than {@code Integer.MAX_VALUE} elements, returns {@code Integer.MAX_VALUE}.
     *
     * @return the number of elements in this list
     */
    int size();

    /**
     * Returns {@code true} if this list contains no elements.
     *
     * @return {@code true} if this list contains no elements
     */
    boolean isEmpty();

    /**
     * Returns the first element of the list.
     *
     * @return the first element of the list
     */
    E getFirst();

    /**
     * Returns the last element of the list.
     *
     * @return the last element of the list
     */
    E getLast();
}
