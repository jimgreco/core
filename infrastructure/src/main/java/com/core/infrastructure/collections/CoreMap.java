package com.core.infrastructure.collections;

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The {@code CoreMap} is a subclass of {@code UnifiedMap} that provides entry set, key set, values collections and
 * their iterators.
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
 * @param <K> the key type
 * @param <V> the value type
 */
@SuppressWarnings("unchecked")
public class CoreMap<K, V> extends UnifiedMap<K, V> {

    private ReusableEntrySet entrySet;
    private ReusableKeySet keySet;
    private ReusableValuesCollection valuesCollection;

    /**
     * Constructs an empty map with an initial capacity of 8.
     */
    public CoreMap() {
        super();
    }

    /**
     * Constructs a map with the specified initial capacity.
     *
     * @param initialCapacity the initial capacity of the map
     */
    public CoreMap(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Constructs a map with the specified initial capacity and load factor.
     *
     * @param initialCapacity the initial capacity of the map
     * @param loadFactor the map's load factor before re-hashing
     */
    public CoreMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    /**
     * Constructs a map from the entries in another map.
     *
     * @param map the map
     */
    public CoreMap(Map<? extends K, ? extends V> map) {
        super(map);
    }

    /**
     * Constructs a map from the specified key/value pairs.
     *
     * @param pairs the key/value pairs
     */
    public CoreMap(Pair<K, V>... pairs) {
        super(pairs);
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new ReusableEntrySet();
        }
        return entrySet;
    }

    @Override
    public Set<K> keySet() {
        if (keySet == null) {
            keySet = new ReusableKeySet();
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        if (valuesCollection == null) {
            valuesCollection = new ReusableValuesCollection();
        }
        return valuesCollection;
    }

    private K nonSentinel(Object key) {
        return key == NULL_KEY ? null : (K) key;
    }

    private static boolean nullSafeEquals(Object value, Object other) {
        if (value == null) {
            return other == null;
        } else {
            return other == value || value.equals(other);
        }
    }

    /**
     * The key set.
     */
    private class ReusableKeySet extends KeySet {

        private ObjectPool<ReusableKeySetIterator> keySetIteratorPool;

        ReusableKeySet() {
        }

        @Override
        public Iterator<K> iterator() {
            if (keySetIteratorPool == null) {
                keySetIteratorPool = new ObjectPool<>(ReusableKeySetIterator::new, 1);
            }
            var iterator = keySetIteratorPool.borrowObject();
            iterator.start();
            return iterator;
        }
    }

    private class ReusableKeySetIterator implements Iterator<K> {

        // set when the iterator is taken from the pool and cleared when recycled back to the pool
        boolean active;
        ReusableEntrySetIterator iterator;

        ReusableKeySetIterator() {
        }

        @Override
        public boolean hasNext() {
            if (iterator.returnToPool) {
                active = false;
                keySet.keySetIteratorPool.returnObject(this);
            }
            return iterator.hasNext();
        }

        @Override
        public K next() {
            return iterator.next().getKey();
        }

        @Override
        public void remove() {
            iterator.remove();
        }

        void start() {
            active = true;
            iterator = (ReusableEntrySetIterator) entrySet().iterator();
            iterator.start();
        }
    }

    /**
     * The values collection.
     */
    private class ReusableValuesCollection extends ValuesCollection {

        private ObjectPool<ReusableValuesIterator> valuesCollectionIteratorPool;

        ReusableValuesCollection() {
        }

        @Override
        public Iterator<V> iterator() {
            if (valuesCollectionIteratorPool == null) {
                valuesCollectionIteratorPool = new ObjectPool<>(ReusableValuesIterator::new, 1);
            }
            var iterator = valuesCollectionIteratorPool.borrowObject();
            iterator.start();
            return iterator;
        }
    }

    private class ReusableValuesIterator implements Iterator<V> {

        // set when the iterator is taken from the pool and cleared when recycled back to the pool
        boolean active;
        ReusableEntrySetIterator iterator;

        ReusableValuesIterator() {
        }

        @Override
        public boolean hasNext() {
            if (iterator.returnToPool) {
                active = false;
                valuesCollection.valuesCollectionIteratorPool.returnObject(this);
            }
            return iterator.hasNext();
        }

        @Override
        public V next() {
            return iterator.next().getValue();
        }

        @Override
        public void remove() {
            iterator.remove();
        }

        void start() {
            active = true;
            iterator = (ReusableEntrySetIterator) entrySet().iterator();
            iterator.start();
        }
    }

    /**
     * The entry set.
     */
    private class ReusableEntrySet extends EntrySet {

        private ObjectPool<ReusableEntrySetIterator> entrySetIteratorPool;

        ReusableEntrySet() {
            super();
        }

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            if (entrySetIteratorPool == null) {
                entrySetIteratorPool = new ObjectPool<>(ReusableEntrySetIterator::new, 1);
            }
            var iterator = entrySetIteratorPool.borrowObject();
            iterator.start();
            return iterator;
        }
    }

    private class ReusableEntrySetIterator extends PositionalIterator<Map.Entry<K, V>> {

        private final ResusableEntry entry;
        // set when the iterator is taken from the pool and cleared when recycled back to the pool
        private boolean active;
        private boolean returnToPool;

        ReusableEntrySetIterator() {
            super();
            entry = new ResusableEntry();
        }

        @Override
        public boolean hasNext() {
            if (!active) {
                return false;
            }
            if (returnToPool) {
                returnToPool();
            }

            return count < CoreMap.this.size();
        }

        @Override
        public Map.Entry<K, V> next() {
            if (!active) {
                throw new IllegalStateException();
            }
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            count++;
            var table = CoreMap.this.table;
            if (chainPosition != 0) {
                return nextFromChain();
            }
            while (table[position] == null) {
                position += 2;
            }
            var cur = table[position];
            final var value = table[position + 1];
            if (cur == CHAINED_KEY) {
                return nextFromChain();
            }
            position += 2;
            lastReturned = true;
            entry.key = CoreMap.this.nonSentinel(cur);
            entry.value = (V) value;

            if (count == CoreMap.this.size()) {
                returnToPool = true;
            }

            return entry;
        }

        @Override
        public void remove() {
            if (!active) {
                throw new IllegalStateException();
            }
            super.remove();
        }

        void start() {
            active = true;
            count = 0;
            position = 0;
            chainPosition = 0;
            lastReturned = false;
        }

        private void returnToPool() {
            active = false;
            returnToPool = false;
            entrySet.entrySetIteratorPool.returnObject(this);
        }

        protected Map.Entry<K, V> nextFromChain() {
            var chain = (Object[]) CoreMap.this.table[this.position + 1];
            final var cur = chain[chainPosition];
            final var value = chain[chainPosition + 1];
            chainPosition += 2;
            if (chainPosition >= chain.length || chain[chainPosition] == null) {
                chainPosition = 0;
                position += 2;
            }
            lastReturned = true;
            entry.key = CoreMap.this.nonSentinel(cur);
            entry.value = (V) value;
            return entry;
        }
    }

    private class ResusableEntry implements Map.Entry<K, V> {

        K key;
        V value;

        ResusableEntry() {
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            this.value = value;
            if (CoreMap.this.containsKey(key)) {
                return CoreMap.this.put(key, value);
            }
            return null;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Map.Entry) {
                var other = (Map.Entry<?, ?>) obj;
                var otherKey = (K) other.getKey();
                var otherValue = (V) other.getValue();
                return nullSafeEquals(key, otherKey) && nullSafeEquals(value, otherValue);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return (key == null ? 0 : key.hashCode()) ^ (value == null ? 0 : value.hashCode());
        }

        @Override
        public String toString() {
            return key + "=" + value;
        }
    }
}