package com.core.infrastructure.collections;

import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;

import java.util.Iterator;

/**
 * An ordered doubly linked list.
 *
 * <p>The list has an iterator that resets to the first element of the list on each invocation.
 * As such, only a single iterator can be active at any given time.
 *
 * @param <E> the type of elements held in this list
 */
public class LinkedList<E> implements Iterable<E>, Deque<E>, Encodable {

    private final IntrusiveLinkedList<Node<E>> list;
    private final ObjectPool<Node<E>> nodePool;
    private It<E> iterator;

    /**
     * Constructs an empty linked list with an initial capacity of 10.
     */
    public LinkedList() {
        super();
        list = new IntrusiveLinkedList<>();
        nodePool = new ObjectPool<>(Node::new);
    }

    /**
     * Constructs an empty linked list with a specified initial capacity.
     *
     * @param initialCapacity the initial capacity of the list
     */
    public LinkedList(int initialCapacity) {
        super();
        list = new IntrusiveLinkedList<>();
        nodePool = new ObjectPool<>(Node::new, initialCapacity);
    }

    /**
     * Creates a linked list from the specified list of {@code items}.
     *
     * @param <T> the item type
     * @param items the items to add to the linked list
     * @return the list
     */
    @SuppressWarnings("unchecked")
    public static <T> LinkedList<T> of(T... items) {
        var list = new LinkedList<T>();
        for (var item : items) {
            list.addLast(item);
        }
        return list;
    }

    /**
     * Inserts the specified {@code item} into the list.
     * All items in the list are {@link Comparable}s and as such the position of the specified item in the list will be
     * determined by comparing the item to each element in the list.
     *
     * @param item the item
     */
    public void insert(E item) {
        list.insert(createNode(item));
    }

    /**
     * Removes the first node in the list that is equal to the specified {@code item}.
     *
     * @param item the item
     * @return if the item was removed
     */
    public boolean remove(E item) {
        var next = list.getFirst();
        while (next != null) {
            if (next.item.equals(item)) {
                list.remove(next);
                nodePool.returnObject(next);
                return true;
            }
            var nextFull = next.getNext();
            next = nextFull == null ? null : nextFull.getItem();
        }
        return false;
    }

    /**
     * Returns the item from the list at the specified {@code index}.
     *
     * @param index the item's index
     * @return the item
     */
    public E get(int index) {
        return list.get(index).item;
    }

    /**
     * Clears all elements of the list.
     */
    public void clear() {
        while (!isEmpty()) {
            removeLast();
        }
    }

    @Override
    public boolean contains(E item) {
        var next = list.getFirst();
        while (next != null) {
            if (next.item == item) {
                return true;
            }
            next = next.getNext() == null ? null : next.getNext().getItem();

        }
        return false;
    }

    @Override
    public void addFirst(E item) {
        list.addFirst(createNode(item));
    }

    @Override
    public void addLast(E item) {
        list.addLast(createNode(item));
    }

    @Override
    public E removeFirst() {
        return removeNode(list.removeFirst());
    }

    @Override
    public E removeLast() {
        return removeNode(list.removeLast());
    }

    @Override
    public E getFirst() {
        var item = list.getFirst();
        return item == null ? null : item.item;
    }

    @Override
    public E getLast() {
        var item = list.getLast();
        return item == null ? null : item.item;
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public void encode(ObjectEncoder encoder) {
        list.encode(encoder);
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    private Node<E> createNode(E item) {
        var node = nodePool.borrowObject();
        node.item = item;
        return node;
    }

    private E removeNode(Node<E> node) {
        var item = node.item;
        nodePool.returnObject(node);
        return item;
    }

    @Override
    public Iterator<E> iterator() {
        if (iterator == null) {
            iterator = new It<>();
        }
        iterator.iterator = list.iterator();
        return iterator;
    }

    private static class It<T> implements Iterator<T> {

        private Iterator<Node<T>> iterator;

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            return iterator.next().item;
        }

        @Override
        public void remove() {
            iterator.remove();
        }
    }

    private static class Node<T>
            implements IntrusiveLinkedList.IntrusiveLinkedListItem<Node<T>>, Resettable, Encodable {

        private IntrusiveLinkedList.IntrusiveLinkedListItem<Node<T>> previous;
        private IntrusiveLinkedList.IntrusiveLinkedListItem<Node<T>> next;
        T item;

        @Override
        public IntrusiveLinkedList.IntrusiveLinkedListItem<Node<T>> getPrevious() {
            return previous;
        }

        @Override
        public IntrusiveLinkedList.IntrusiveLinkedListItem<Node<T>> getNext() {
            return next;
        }

        @Override
        public void setPrevious(IntrusiveLinkedList.IntrusiveLinkedListItem<Node<T>> prev) {
            this.previous = prev;
        }

        @Override
        public void setNext(IntrusiveLinkedList.IntrusiveLinkedListItem<Node<T>> next) {
            this.next = next;
        }

        @Override
        public Node<T> getItem() {
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compareTo(Node<T> o) {
            // this is annoying, but necessary because of the generic parameters
            return ((Comparable<T>) item).compareTo(o.item);
        }

        @Override
        public void reset() {
            this.next = null;
            this.previous = null;
            this.item = null;
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.object(item);
        }

        @Override
        public String toString() {
            return toEncodedString();
        }
    }
}
