package com.core.infrastructure.collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;

public class CoreMapTest {

    private CoreMap<String, String> map;

    @BeforeEach
    void before_each() {
        map = new CoreMap<>();
        map.put("1", "A");
        map.put("2", "B");
        map.put("3", "C");
    }

    @Nested
    class KeysSetTests {

        @Test
        void iterable_once() {
            var actual = new ArrayList<String>();

            for (var e : map.keySet()) {
                actual.add(e);
            }

            then(actual).containsExactlyInAnyOrder("1", "2", "3");
        }

        @Test
        void iterator_once() {
            var actual = new ArrayList<String>();
            var iterator = map.keySet().iterator();

            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }

            then(actual).containsExactlyInAnyOrder("1", "2", "3");
        }

        @Test
        void iterator_is_reused() {
            var iterator1 = map.keySet().iterator();
            while (iterator1.hasNext()) {
                iterator1.next();
            }

            var iterator2 = map.keySet().iterator();

            then(iterator1).isSameAs(iterator2);
        }

        @Test
        void iterable_multiple() {
            var actual1 = new ArrayList<String>();
            var actual2 = new ArrayList<String>();
            for (var e : map.keySet()) {
                actual1.add(e);
            }

            for (var e : map.keySet()) {
                actual2.add(e);
            }

            then(actual1).containsExactlyInAnyOrder("1", "2", "3");
            then(actual2).containsExactlyInAnyOrder("1", "2", "3");
        }

        @Test
        void iterator_multiple() {
            var actual1 = new ArrayList<String>();
            var actual2 = new ArrayList<String>();
            var iterator1 = map.keySet().iterator();
            while (iterator1.hasNext()) {
                actual1.add(iterator1.next());
            }
            var iterator2 = map.keySet().iterator();

            while (iterator2.hasNext()) {
                actual2.add(iterator2.next());
            }

            then(actual1).containsExactlyInAnyOrder("1", "2", "3");
            then(actual2).containsExactlyInAnyOrder("1", "2", "3");
        }

        @Test
        void iterator_multiple_at_same_time() {
            var actual1 = new ArrayList<String>();
            var actual2 = new ArrayList<String>();
            var iterator1 = map.keySet().iterator();
            var iterator2 = map.keySet().iterator();

            actual1.add(iterator1.next());
            actual1.add(iterator1.next());
            actual2.add(iterator2.next());
            actual2.add(iterator2.next());
            actual2.add(iterator2.next());
            iterator2.hasNext();
            actual1.add(iterator1.next());
            iterator1.hasNext();

            then(actual1).containsExactlyInAnyOrder("1", "2", "3");
            then(actual2).containsExactlyInAnyOrder("1", "2", "3");
        }

        @Test
        void automatically_returned_iterator_throws_IllegalStateException() {
            var iterator = map.keySet().iterator();

            while (iterator.hasNext()) {
                iterator.next();
            }

            thenThrownBy(iterator::next).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void too_many_next_throws_NoSuchElementException() {
            var iterator = map.keySet().iterator();
            iterator.next();
            iterator.next();
            iterator.next();

            thenThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }

        @Test
        void remove_after_recycling_throws_IllegalStateException() {
            var iterator = map.keySet().iterator();
            while (iterator.hasNext()) {
                iterator.next();
            }

            thenThrownBy(iterator::remove).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void remove_first_element_throws_IllegalStateException() {
            var iterator = map.keySet().iterator();

            thenThrownBy(iterator::remove).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void remove_element_twice_throws_IllegalStateException() {
            var iterator = map.keySet().iterator();
            iterator.next();

            iterator.remove();

            thenThrownBy(iterator::remove).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void remove_first_element() {
            var iterator = map.keySet().iterator();
            iterator.hasNext();
            var el1 = iterator.next();

            iterator.remove();

            then(map.size()).isEqualTo(2);
            var el2 = iterator.next();
            var el3 = iterator.next();
            then(List.of(el1, el2, el3)).containsExactlyInAnyOrder("1", "2", "3");
        }

        @Test
        void remove_middle_element() {
            var iterator = map.keySet().iterator();
            iterator.hasNext();
            final var el1 = iterator.next();
            iterator.hasNext();
            final var el2 = iterator.next();

            iterator.remove();

            then(map.size()).isEqualTo(2);
            var el3 = iterator.next();
            then(List.of(el1, el2, el3)).containsExactlyInAnyOrder("1", "2", "3");
        }

        @Test
        void remove_last_element() {
            var iterator = map.keySet().iterator();
            iterator.hasNext();
            final var el1 = iterator.next();
            iterator.hasNext();
            final var el2 = iterator.next();
            iterator.hasNext();
            final var el3 = iterator.next();

            iterator.remove();

            then(map.size()).isEqualTo(2);
            then(List.of(el1, el2, el3)).containsExactlyInAnyOrder("1", "2", "3");
        }
    }

    @Nested
    class ValuesCollectionTests {

        @Test
        void iterable_once() {
            var actual = new ArrayList<String>();

            for (var e : map) {
                actual.add(e);
            }

            then(actual).containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void iterator_once() {
            var actual = new ArrayList<String>();
            var iterator = map.iterator();

            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }

            then(actual).containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void iterator_is_reused() {
            var iterator1 = map.iterator();
            while (iterator1.hasNext()) {
                iterator1.next();
            }

            var iterator2 = map.iterator();

            then(iterator1).isSameAs(iterator2);
        }

        @Test
        void iterable_multiple() {
            var actual1 = new ArrayList<String>();
            var actual2 = new ArrayList<String>();
            for (var e : map) {
                actual1.add(e);
            }

            for (var e : map) {
                actual2.add(e);
            }

            then(actual1).containsExactlyInAnyOrder("A", "B", "C");
            then(actual2).containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void iterator_multiple() {
            var actual1 = new ArrayList<String>();
            var actual2 = new ArrayList<String>();
            var iterator1 = map.iterator();
            while (iterator1.hasNext()) {
                actual1.add(iterator1.next());
            }
            var iterator2 = map.iterator();

            while (iterator2.hasNext()) {
                actual2.add(iterator2.next());
            }

            then(actual1).containsExactlyInAnyOrder("A", "B", "C");
            then(actual2).containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void iterator_multiple_at_same_time() {
            var actual1 = new ArrayList<String>();
            var actual2 = new ArrayList<String>();
            var iterator1 = map.iterator();
            var iterator2 = map.iterator();

            actual1.add(iterator1.next());
            actual1.add(iterator1.next());
            actual2.add(iterator2.next());
            actual2.add(iterator2.next());
            actual2.add(iterator2.next());
            iterator2.hasNext();
            actual1.add(iterator1.next());
            iterator1.hasNext();

            then(actual1).containsExactlyInAnyOrder("A", "B", "C");
            then(actual2).containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void automatically_returned_iterator_throws_IllegalStateException() {
            var iterator = map.iterator();

            while (iterator.hasNext()) {
                iterator.next();
            }

            thenThrownBy(iterator::next).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void too_many_next_throws_NoSuchElementException() {
            var iterator = map.iterator();
            iterator.next();
            iterator.next();
            iterator.next();

            thenThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }

        @Test
        void remove_after_recycling_throws_IllegalStateException() {
            var iterator = map.iterator();
            while (iterator.hasNext()) {
                iterator.next();
            }

            thenThrownBy(iterator::remove).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void remove_first_element_throws_IllegalStateException() {
            var iterator = map.iterator();

            thenThrownBy(iterator::remove).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void remove_element_twice_throws_IllegalStateException() {
            var iterator = map.iterator();
            iterator.next();

            iterator.remove();

            thenThrownBy(iterator::remove).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void remove_first_element() {
            var iterator = map.iterator();
            iterator.hasNext();
            final var el1 = iterator.next();

            iterator.remove();

            then(map.size()).isEqualTo(2);
            var el2 = iterator.next();
            var el3 = iterator.next();
            then(List.of(el1, el2, el3)).containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void remove_middle_element() {
            var iterator = map.iterator();
            iterator.hasNext();
            final var el1 = iterator.next();
            iterator.hasNext();
            final var el2 = iterator.next();

            iterator.remove();

            then(map.size()).isEqualTo(2);
            var el3 = iterator.next();
            then(List.of(el1, el2, el3)).containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void remove_last_element() {
            var iterator = map.iterator();
            iterator.hasNext();
            final var el1 = iterator.next();
            iterator.hasNext();
            final var el2 = iterator.next();
            iterator.hasNext();
            final var el3 = iterator.next();

            iterator.remove();

            then(map.size()).isEqualTo(2);
            then(List.of(el1, el2, el3)).containsExactlyInAnyOrder("A", "B", "C");
        }
    }

    @Nested
    class EntrySetTests {

        @Test
        void iterable_once() {
            var actualKeys = new ArrayList<String>();
            var actualValues = new ArrayList<String>();

            for (var e : map.entrySet()) {
                actualKeys.add(e.getKey());
                actualValues.add(e.getValue());
            }

            then(actualKeys).containsExactlyInAnyOrder("1", "2", "3");
            then(actualValues).containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void iterator_once() {
            var actualKeys = new ArrayList<String>();
            var actualValues = new ArrayList<String>();
            var iterator = map.entrySet().iterator();

            while (iterator.hasNext()) {
                var e = iterator.next();
                actualKeys.add(e.getKey());
                actualValues.add(e.getValue());
            }

            then(actualKeys).containsExactlyInAnyOrder("1", "2", "3");
            then(actualValues).containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void iterator_is_reused() {
            var iterator1 = map.entrySet().iterator();
            while (iterator1.hasNext()) {
                iterator1.next();
            }

            var iterator2 = map.entrySet().iterator();

            then(iterator1).isSameAs(iterator2);
        }

        @Test
        void iterable_multiple() {
            var actual1Keys = new ArrayList<String>();
            var actual1Values = new ArrayList<String>();
            var actual2Keys = new ArrayList<String>();
            var actual2Values = new ArrayList<String>();
            for (var e : map.entrySet()) {
                actual1Keys.add(e.getKey());
                actual1Values.add(e.getValue());
            }

            for (var e : map.entrySet()) {
                actual2Keys.add(e.getKey());
                actual2Values.add(e.getValue());
            }

            then(actual1Keys).containsExactlyInAnyOrder("1", "2", "3");
            then(actual1Values).containsExactlyInAnyOrder("A", "B", "C");
            then(actual2Keys).containsExactlyInAnyOrder("1", "2", "3");
            then(actual2Values).containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void iterator_multiple() {
            var actual1Keys = new ArrayList<String>();
            var actual1Values = new ArrayList<String>();
            var actual2Keys = new ArrayList<String>();
            var actual2Values = new ArrayList<String>();
            var iterator1 = map.entrySet().iterator();
            while (iterator1.hasNext()) {
                var e = iterator1.next();
                actual1Keys.add(e.getKey());
                actual1Values.add(e.getValue());
            }
            var iterator2 = map.entrySet().iterator();

            while (iterator2.hasNext()) {
                var e = iterator2.next();
                actual2Keys.add(e.getKey());
                actual2Values.add(e.getValue());
            }

            then(actual1Keys).containsExactlyInAnyOrder("1", "2", "3");
            then(actual1Values).containsExactlyInAnyOrder("A", "B", "C");
            then(actual2Keys).containsExactlyInAnyOrder("1", "2", "3");
            then(actual2Values).containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void iterator_multiple_at_same_time() {
            final var actual1Keys = new ArrayList<String>();
            final var actual1Values = new ArrayList<String>();
            final var actual2Keys = new ArrayList<String>();
            final var actual2Values = new ArrayList<String>();
            final var iterator1 = map.entrySet().iterator();
            final var iterator2 = map.entrySet().iterator();

            var e = iterator1.next();
            actual1Keys.add(e.getKey());
            actual1Values.add(e.getValue());
            e = iterator1.next();
            actual1Keys.add(e.getKey());
            actual1Values.add(e.getValue());
            e = iterator2.next();
            actual2Keys.add(e.getKey());
            actual2Values.add(e.getValue());
            e = iterator2.next();
            actual2Keys.add(e.getKey());
            actual2Values.add(e.getValue());
            e = iterator2.next();
            actual2Keys.add(e.getKey());
            actual2Values.add(e.getValue());
            iterator2.hasNext();
            e = iterator1.next();
            actual1Keys.add(e.getKey());
            actual1Values.add(e.getValue());
            iterator1.hasNext();

            then(actual1Keys).containsExactlyInAnyOrder("1", "2", "3");
            then(actual1Values).containsExactlyInAnyOrder("A", "B", "C");
            then(actual2Keys).containsExactlyInAnyOrder("1", "2", "3");
            then(actual2Values).containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void automatically_returned_iterator_throws_IllegalStateException() {
            var iterator = map.entrySet().iterator();

            while (iterator.hasNext()) {
                iterator.next();
            }

            thenThrownBy(iterator::next).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void too_many_next_throws_NoSuchElementException() {
            var iterator = map.entrySet().iterator();
            iterator.next();
            iterator.next();
            iterator.next();

            thenThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }

        @Test
        void remove_after_recycling_throws_IllegalStateException() {
            var iterator = map.entrySet().iterator();
            while (iterator.hasNext()) {
                iterator.next();
            }

            thenThrownBy(iterator::remove).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void remove_first_element_throws_IllegalStateException() {
            var iterator = map.entrySet().iterator();

            thenThrownBy(iterator::remove).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void remove_element_twice_throws_IllegalStateException() {
            var iterator = map.entrySet().iterator();
            iterator.next();

            iterator.remove();

            thenThrownBy(iterator::remove).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void remove_first_element() {
            var actualKeys = new ArrayList<String>();
            var actualValues = new ArrayList<String>();
            var iterator = map.entrySet().iterator();
            iterator.hasNext();
            var el = iterator.next();
            actualKeys.add(el.getKey());
            actualValues.add(el.getValue());

            iterator.remove();

            then(map.size()).isEqualTo(2);
            el = iterator.next();
            actualKeys.add(el.getKey());
            actualValues.add(el.getValue());
            el = iterator.next();
            actualKeys.add(el.getKey());
            actualValues.add(el.getValue());
            then(actualKeys).containsExactlyInAnyOrder("1", "2", "3");
            then(actualValues).containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void remove_middle_element() {
            var actualKeys = new ArrayList<String>();
            var actualValues = new ArrayList<String>();
            var iterator = map.entrySet().iterator();
            iterator.hasNext();
            var el = iterator.next();
            actualKeys.add(el.getKey());
            actualValues.add(el.getValue());
            iterator.hasNext();
            el = iterator.next();
            actualKeys.add(el.getKey());
            actualValues.add(el.getValue());

            iterator.remove();

            then(map.size()).isEqualTo(2);
            el = iterator.next();
            actualKeys.add(el.getKey());
            actualValues.add(el.getValue());
            then(actualKeys).containsExactlyInAnyOrder("1", "2", "3");
            then(actualValues).containsExactlyInAnyOrder("A", "B", "C");
        }

        @Test
        void remove_last_element() {
            var actualKeys = new ArrayList<String>();
            var actualValues = new ArrayList<String>();
            var iterator = map.entrySet().iterator();
            iterator.hasNext();
            var el = iterator.next();
            actualKeys.add(el.getKey());
            actualValues.add(el.getValue());
            iterator.hasNext();
            el = iterator.next();
            actualKeys.add(el.getKey());
            actualValues.add(el.getValue());
            iterator.hasNext();
            el = iterator.next();
            actualKeys.add(el.getKey());
            actualValues.add(el.getValue());

            iterator.remove();

            then(map.size()).isEqualTo(2);
            then(actualKeys).containsExactlyInAnyOrder("1", "2", "3");
            then(actualValues).containsExactlyInAnyOrder("A", "B", "C");
        }
    }
}
