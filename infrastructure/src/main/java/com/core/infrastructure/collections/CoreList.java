package com.core.infrastructure.collections;

import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * The {@code CoreList} is a subclass of {@code FastList} that provides reusable iterators.
 * It is primarily intended for use inside of applications that will use the iterator once in a method call and then
 * expect that iterator to be garbage collected.
 *
 * <p>Typically, a user iterates by checking to see if the iterator has a next element by invoking the
 * ({@link Iterator#hasNext()} method.
 * If true, then {@link Iterator#next()} is invoked to retrieve the next element.
 * If false, there are no more elements to iterate and the iterator is no longer be used.
 * With reusable iterators, the iterator is recycled back into the pool to be reused on the next invocation of the
 * iterator.
 *
 * @param <T> the element type
 */
public class CoreList<T> extends FastList<T> {

    private final boolean reuseIterators;
    private ObjectPool<ReusableIterator> iteratorPool;

    /**
     * Constructs an empty list.
     */
    public CoreList() {
        super();
        reuseIterators = Boolean.getBoolean("core.reuseIterators");
    }

    /**
     * Constructs a list with the specified initial capacity.
     *
     * @param initialCapacity the initial capacity of the list
     */
    public CoreList(int initialCapacity) {
        super(initialCapacity);
        reuseIterators = Boolean.getBoolean("core.reuseIterators");
    }

    /**
     * Constructs a list with the elements of a source collection.
     *
     * @param source the source collection
     */
    public CoreList(Collection<? extends T> source) {
        super(source);
        reuseIterators = Boolean.getBoolean("core.reuseIterators");
    }

    @Override
    public Iterator<T> iterator() {
        if (reuseIterators) {
            if (iteratorPool == null) {
                iteratorPool = new ObjectPool<>(ReusableIterator::new, 1);
            }
            var iterator = iteratorPool.borrowObject();
            iterator.start();
            return iterator;
        } else {
            return super.iterator();
        }
    }

    /**
     * Returns an iterator that was not automatically returned to the pool during iteration.
     *
     * @param iterator the iterator
     */
    public void returnIterator(Iterator<T> iterator) {
        ((ReusableIterator) iterator).returnToPool();
    }

    private class ReusableIterator implements Iterator<T> {

        protected boolean returnToPool;
        // set when the iterator is taken from the pool and cleared when recycled back to the pool
        protected boolean active;
        protected int currentIndex;
        protected int lastIndex;

        ReusableIterator() {
        }

        @Override
        public boolean hasNext() {
            if (!active) {
                return false;
            }
            if (returnToPool) {
                returnToPool();
            }

            return currentIndex != size;
        }

        @Override
        public T next() {
            if (!active) {
                throw new IllegalStateException();
            }
            if (currentIndex >= size) {
                throw new NoSuchElementException();
            }

            var next = get(currentIndex);
            lastIndex = currentIndex++;

            if (currentIndex == size) {
                returnToPool = true;
            }

            return next;
        }

        @Override
        public void remove() {
            if (!active) {
                throw new IllegalStateException();
            }
            if (lastIndex == -1) {
                throw new IllegalStateException();
            }

            CoreList.this.remove(lastIndex);
            if (lastIndex < currentIndex) {
                currentIndex--;
            }
            lastIndex = -1;
        }

        void start() {
            active = true;
            currentIndex = 0;
            lastIndex = -1;
        }

        private void returnToPool() {
            active = false;
            returnToPool = false;
            iteratorPool.returnObject(this);
        }
    }
}
