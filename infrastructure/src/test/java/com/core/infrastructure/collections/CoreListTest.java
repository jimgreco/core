package com.core.infrastructure.collections;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;

public class CoreListTest {

    private CoreList<String> list;

    @BeforeEach
    void before_each() {
        System.setProperty("core.reuseIterators", "true");
        list = new CoreList<>();
        list.add("A");
        list.add("B");
        list.add("C");
    }

    @AfterEach
    void after_each() {
        System.setProperty("core.reuseIterators", "false");
    }

    @Test
    void iterable_once() {
        var actual = new ArrayList<String>();

        for (var e : list) {
            actual.add(e);
        }

        then(actual).containsExactly("A", "B", "C");
    }

    @Test
    void iterator_once() {
        var actual = new ArrayList<String>();
        var iterator = list.iterator();

        while (iterator.hasNext()) {
            actual.add(iterator.next());
        }

        then(actual).containsExactly("A", "B", "C");
    }

    @Test
    void iterator_is_reused() {
        var iterator1 = list.iterator();
        while (iterator1.hasNext()) {
            iterator1.next();
        }

        var iterator2 = list.iterator();

        then(iterator1).isSameAs(iterator2);
    }

    @Test
    void iterable_multiple() {
        var actual1 = new ArrayList<String>();
        var actual2 = new ArrayList<String>();
        for (var e : list) {
            actual1.add(e);
        }

        for (var e : list) {
            actual2.add(e);
        }

        then(actual1).containsExactly("A", "B", "C");
        then(actual2).containsExactly("A", "B", "C");
    }

    @Test
    void iterator_multiple() {
        var actual1 = new ArrayList<String>();
        var actual2 = new ArrayList<String>();
        var iterator1 = list.iterator();
        while (iterator1.hasNext()) {
            actual1.add(iterator1.next());
        }
        var iterator2 = list.iterator();

        while (iterator2.hasNext()) {
            actual2.add(iterator2.next());
        }

        then(actual1).containsExactly("A", "B", "C");
        then(actual2).containsExactly("A", "B", "C");
    }

    @Test
    void iterator_multiple_at_same_time() {
        var actual1 = new ArrayList<String>();
        var actual2 = new ArrayList<String>();
        var iterator1 = list.iterator();
        var iterator2 = list.iterator();

        actual1.add(iterator1.next());
        actual1.add(iterator1.next());
        actual2.add(iterator2.next());
        actual2.add(iterator2.next());
        actual2.add(iterator2.next());
        iterator2.hasNext();
        actual1.add(iterator1.next());
        iterator1.hasNext();

        then(actual1).containsExactly("A", "B", "C");
        then(actual2).containsExactly("A", "B", "C");
    }

    @Test
    void returnIterator_returns_iterator_to_pool() {
        var iterator = list.iterator();
        iterator.next();
        iterator.next();

        list.returnIterator(iterator);

        thenThrownBy(iterator::next).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void automatically_returned_iterator_throws_IllegalStateException() {
        var iterator = list.iterator();

        while (iterator.hasNext()) {
            iterator.next();
        }

        thenThrownBy(iterator::next).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void too_many_next_throws_NoSuchElementException() {
        var iterator = list.iterator();
        iterator.next();
        iterator.next();
        iterator.next();

        thenThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void remove_after_recycling_throws_IllegalStateException() {
        var iterator = list.iterator();
        while (iterator.hasNext()) {
            iterator.next();
        }

        thenThrownBy(iterator::remove).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void remove_first_element_throws_IllegalStateException() {
        var iterator = list.iterator();

        thenThrownBy(iterator::remove).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void remove_element_twice_throws_IllegalStateException() {
        var iterator = list.iterator();
        iterator.next();

        iterator.remove();

        thenThrownBy(iterator::remove).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void remove_first_element() {
        var iterator = list.iterator();
        iterator.hasNext();
        iterator.next();

        iterator.remove();

        then(list.size()).isEqualTo(2);
        then(list.get(0)).isEqualTo("B");
        then(list.get(1)).isEqualTo("C");
    }

    @Test
    void remove_middle_element() {
        var iterator = list.iterator();
        iterator.hasNext();
        iterator.next();
        iterator.hasNext();
        iterator.next();

        iterator.remove();

        then(list.size()).isEqualTo(2);
        then(list.get(0)).isEqualTo("A");
        then(list.get(1)).isEqualTo("C");
    }

    @Test
    void remove_last_element() {
        var iterator = list.iterator();
        iterator.hasNext();
        iterator.next();
        iterator.hasNext();
        iterator.next();
        iterator.hasNext();
        iterator.next();

        iterator.remove();

        then(list.size()).isEqualTo(2);
        then(list.get(0)).isEqualTo("A");
        then(list.get(1)).isEqualTo("B");
    }
}
