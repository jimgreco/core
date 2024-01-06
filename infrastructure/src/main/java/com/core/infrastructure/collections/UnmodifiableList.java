package com.core.infrastructure.collections;

import java.util.AbstractList;
import java.util.List;

/**
 * A read-only list that maps a modifiable list.
 * This allows the an API to modify the underlying {@code List} object, while presenting a list that cannot be modified
 * to the API user.
 *
 * @param <E> the list element type
 */
public class UnmodifiableList<E> extends AbstractList<E> {

    private final List<E> list;

    /**
     * Constructs a {@code UnmodifiableList} that wraps a new {@code CoreList}.
     */
    public UnmodifiableList() {
        this(new CoreList<>());
    }

    /**
     * Constructs a {@code UnmodifiableList} with the specified {@code list}.
     *
     * @param list the list
     */
    public UnmodifiableList(List<E> list) {
        this.list = list;
    }

    /**
     * Returns the underlying list that is modifiable.
     *
     * @return the underlying list that is modifiable
     */
    public List<E> getUnderlyingList() {
        return list;
    }

    @Override
    public E get(int index) {
        return list.get(index);
    }

    @Override
    public int size() {
        return list.size();
    }
}
