package com.core.infrastructure.collections;

import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

/**
 * An ordered intrusive doubly linked list.
 *
 * <p>An intrusive doubly linked list contains the previous and next pointers as fields on the object.
 * When an item is inserted into the list, the previous and next pointers are set.
 * When an item is removed from the list, the previous and next pointers are nulled.
 * As such, the user of the list must be careful to not insert an element into the list twice as this will cause the
 * order of the list to break.
 *
 * <p>The list is ordered through a {@link Comparable}, which all items must implement.
 *
 * <p>The list has an iterator that resets to the first element of the list on each invocation.
 * As such, only a single iterator can be active at any given time.
 *
 * @param <E> the type of elements held in this list
 */
public class IntrusiveLinkedList<E extends IntrusiveLinkedList.IntrusiveLinkedListItem<E>> implements
        Iterable<E>, Deque<E>, Encodable {

    private IntrusiveLinkedListItem<E> head;
    private IntrusiveLinkedListItem<E> tail;
    private int size;
    private It iterator;

    /**
     * Creates an empty {@code IntrusiveLinkedList}.
     */
    public IntrusiveLinkedList() {
    }

    @Override
    public boolean contains(E item) {
        var next = head;
        while (next != null) {
            if (next == item) {
                return true;
            }
            next = next.getNext();
        }
        return false;
    }

    @Override
    public void addFirst(E item) {
        item.setNext(head);
        if (tail == null) {
            tail = item;
        } else {
            head.setPrevious(item);
        }
        head = item;
        size++;
    }

    @Override
    public void addLast(E item) {
        item.setPrevious(tail);
        if (tail == null) {
            head = item;
        } else {
            tail.setNext(item);
        }
        tail = item;
        size++;
    }

    /**
     * Returns the item from the list at the specified index.
     *
     * @param index the item's index
     * @return the item
     * @throws IndexOutOfBoundsException if the index is less than 0 or greater than or equal to the size of the list
     * @throws IllegalStateException if the item doesn't exist (internal error)
     */
    public E get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }

        var count = 0;
        var next = head;
        while (next != null) {
            if (count == index) {
                return next.getItem();
            }
            next = next.getNext();
            count++;
        }

        throw new IllegalStateException();
    }

    /**
     * Inserts the item into the list.
     * All items in the list are {@link Comparable}s and as such the position of the specified item in the list will be
     * determined by comparing the item to each element in the list.
     *
     * @param item the item to add
     */
    public void insert(E item) {
        IntrusiveLinkedListItem<E> prev = null;
        var next = head;
        while (next != null) {
            if (item.compareTo(next.getItem()) < 0) {
                // insert between prev and next
                item.setPrevious(prev);
                item.setNext(next);
                next.setPrevious(item);
                if (prev == null) {
                    head = item;
                } else {
                    prev.setNext(item);
                }
                if (tail == null) {
                    tail = item;
                }
                size++;
                return;
            }

            // move to next node
            prev = next;
            next = next.getNext();
        }

        // end of the list
        addLast(item);
    }

    /**
     * Removes the item from the list.
     *
     * <p>This method is fast as it uses the item's
     * {@link IntrusiveLinkedList.IntrusiveLinkedListItem#getPrevious() getPrevious()} and
     * {@link IntrusiveLinkedList.IntrusiveLinkedListItem#getNext() getNext()} methods to connect the item's previous
     * item to the item's next item.
     * This method will update the head of the list to the next item if the item is currently the head of the list.
     * This method will update the tail of the list to the previous item if the item is currently the tail of the list.
     * However, no validation is done and if the item is not currently in the list, or the next/previous items have
     * been set to a value outside of this list, then the list may end up in an invalid state.
     *
     * @param item the item to remove
     */
    public void remove(E item) {
        var next = item.getNext();
        var prev = item.getPrevious();
        item.setNext(null);
        item.setPrevious(null);
        if (prev == null) {
            head = next;
        } else {
            prev.setNext(next);
        }
        if (next == null) {
            tail = prev;
        } else {
            next.setPrevious(prev);
        }
        size--;
    }

    @Override
    public E removeFirst() {
        if (head == null) {
            throw new NoSuchElementException();
        }
        var oldHead = head;
        remove(head.getItem());
        return oldHead.getItem();
    }

    /**
     * Removes and returns the first element of the list that matches the {@code predicate}, or null if no element of
     *     the list matches the predicate.
     *
     * @param predicate the predicate
     * @return the first element of the list that matches the {@code predicate}, or null if no element of the list
     *     matches the predicate
     */
    public E removeFirst(Predicate<E> predicate) {
        var next = head;
        while (next != null) {
            if (predicate.test(next.getItem())) {
                remove(next.getItem());
                return next.getItem();
            }
            next = next.getNext();
        }
        return null;
    }

    @Override
    public E removeLast() {
        if (tail == null) {
            throw new NoSuchElementException();
        }
        var oldTail = tail;
        remove(tail.getItem());
        return oldTail.getItem();
    }

    @Override
    public E getFirst() {
        return head == null ? null : head.getItem();
    }

    @Override
    public E getLast() {
        return tail == null ? null : tail.getItem();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openList();

        var next = head;
        while (next != null) {
            encoder.object(next);
            next = next.getNext();
        }

        encoder.closeList();
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    @Override
    public Iterator<E> iterator() {
        if (iterator == null) {
            iterator = new It();
        }
        return iterator.start();
    }

    private class It implements Iterator<E> {

        private IntrusiveLinkedListItem<E> next;
        private IntrusiveLinkedListItem<E> last;

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public E next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            last = next;
            next = next.getNext();
            return last.getItem();
        }

        @Override
        public void remove() {
            if (last == null) {
                throw new IllegalStateException();
            }
            IntrusiveLinkedList.this.remove(last.getItem());
            last = null;
        }

        It start() {
            next = head;
            last = null;
            return this;
        }
    }

    /**
     * An item in an {@code IntrusiveLinkedList}.
     *
     * @param <T> the item type
     */
    public interface IntrusiveLinkedListItem<T> extends Comparable<T> {

        /**
         * Returns the previous item in the list.
         *
         * @return the previous item in the list
         */
        IntrusiveLinkedListItem<T> getPrevious();

        /**
         * Returns the next item in the list.
         *
         * @return the next item in the list
         */
        IntrusiveLinkedListItem<T> getNext();

        /**
         * Sets the previous item in the list.
             *
         * @param prev the previous item in the list
         */
        void setPrevious(IntrusiveLinkedListItem<T> prev);

        /**
         * Sets the next item in the list.
         *
         * @param next the next item in the list
         */
        void setNext(IntrusiveLinkedListItem<T> next);

        /**
         * Returns this.
         *
         * @return this
         */
        T getItem();
    }
}
