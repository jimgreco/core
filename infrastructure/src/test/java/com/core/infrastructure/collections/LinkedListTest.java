package com.core.infrastructure.collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.BDDAssertions.then;

public class LinkedListTest {

    private LinkedList<Item> list;

    @BeforeEach
    void before_each() {
        list = new LinkedList<>();
    }

    @Test
    void size_is_initially_zero() {
        then(list.size()).isEqualTo(0);
        then(list.isEmpty()).isTrue();
    }

    @Test
    void addFirst_adds_to_the_beginning_of_list() {
        list.addFirst(new Item(1));
        list.addFirst(new Item(2));
        list.addFirst(new Item(3));

        then(list.removeFirst().getValue()).isEqualTo(3);
        then(list.removeFirst().getValue()).isEqualTo(2);
        then(list.removeFirst().getValue()).isEqualTo(1);
        then(list.isEmpty()).isTrue();
    }

    @Test
    void addLast_adds_to_end_of_list() {
        list.addLast(new Item(1));
        list.addLast(new Item(2));
        list.addLast(new Item(3));

        then(list.removeFirst().getValue()).isEqualTo(1);
        then(list.removeFirst().getValue()).isEqualTo(2);
        then(list.removeFirst().getValue()).isEqualTo(3);
        then(list.isEmpty()).isTrue();
    }

    @Test
    void removeLast_removes_from_end_of_list() {
        list.addLast(new Item(1));
        list.addLast(new Item(2));
        list.addLast(new Item(3));

        then(list.removeLast().getValue()).isEqualTo(3);
        then(list.removeLast().getValue()).isEqualTo(2);
        then(list.removeLast().getValue()).isEqualTo(1);
        then(list.isEmpty()).isTrue();
    }

    @Test
    void getFirst_does_not_remove_from_the_beginning_of_list() {
        list.addLast(new Item(1));
        list.addLast(new Item(2));
        list.addLast(new Item(3));

        then(list.getFirst().getValue()).isEqualTo(1);
        then(list.getFirst().getValue()).isEqualTo(1);
    }

    @Test
    void getLast_does_not_remove_from_the_end_of_list() {
        list.addLast(new Item(1));
        list.addLast(new Item(2));
        list.addLast(new Item(3));

        then(list.getLast().getValue()).isEqualTo(3);
        then(list.getLast().getValue()).isEqualTo(3);
    }

    @Test
    void insert_into_list() {
        list.insert(new Item(5));
        list.insert(new Item(4));
        list.insert(new Item(7));

        then(list.removeFirst().getValue()).isEqualTo(4);
        then(list.removeFirst().getValue()).isEqualTo(5);
        then(list.removeFirst().getValue()).isEqualTo(7);
        then(list.isEmpty()).isTrue();
    }

    @Test
    void remove_first_item_from_list() {
        list.insert(new Item(5));
        list.insert(new Item(4));
        list.insert(new Item(7));

        list.remove(new Item(4));

        then(list.removeFirst().getValue()).isEqualTo(5);
        then(list.removeFirst().getValue()).isEqualTo(7);
        then(list.isEmpty()).isTrue();
    }

    @Test
    void remove_middle_item_from_list() {
        list.insert(new Item(5));
        list.insert(new Item(4));
        list.insert(new Item(7));

        list.remove(new Item(5));

        then(list.removeFirst().getValue()).isEqualTo(4);
        then(list.removeFirst().getValue()).isEqualTo(7);
        then(list.isEmpty()).isTrue();
    }

    @Test
    void remove_last_item_from_list() {
        list.insert(new Item(5));
        list.insert(new Item(4));
        list.insert(new Item(7));

        list.remove(new Item(7));

        then(list.removeFirst().getValue()).isEqualTo(4);
        then(list.removeFirst().getValue()).isEqualTo(5);
        then(list.isEmpty()).isTrue();
    }

    @Test
    void iterate_through_list_with_iterable() {
        list.addLast(new Item(1));
        list.addLast(new Item(2));
        list.addLast(new Item(3));
        var actual = new ArrayList<>();

        for (var item : list) {
            actual.add(item.getValue());
        }

        then(actual).containsExactly(1, 2, 3);
    }

    @Test
    void iterate_through_list_with_iterator() {
        list.addLast(new Item(1));
        list.addLast(new Item(2));
        list.addLast(new Item(3));
        var actual = new ArrayList<>();
        var iterator = list.iterator();

        actual.add(iterator.next().getValue());
        actual.add(iterator.next().getValue());
        actual.add(iterator.next().getValue());

        then(iterator.hasNext()).isFalse();
        then(actual).containsExactly(1, 2, 3);
    }

    @Test
    void clear_removes_all_elements() {
        list.insert(new Item(5));
        list.insert(new Item(4));
        list.insert(new Item(7));

        list.clear();

        then(list.isEmpty()).isTrue();
    }

    @Test
    void contains_for_element_in_list() {
        var item1 = new Item(1);
        var item2 = new Item(2);
        var item3 = new Item(3);
        list.addLast(item1);
        list.addLast(item2);
        list.addLast(item3);

        then(list.contains(item1)).isTrue();
        then(list.contains(item2)).isTrue();
        then(list.contains(item3)).isTrue();
    }

    @Test
    void contains_for_null_is_false() {
        var item1 = new Item(1);
        var item2 = new Item(2);
        list.addLast(item1);
        list.addLast(item2);

        then(list.contains(null)).isFalse();
    }

    @Test
    void contains_for_unknown_element_is_false() {
        var item1 = new Item(1);
        var item2 = new Item(2);
        var item3 = new Item(3);
        list.addLast(item1);
        list.addLast(item2);

        then(list.contains(item3)).isFalse();
    }

    private static class Item implements Comparable<Item> {

        int value;

        public Item(int value) {
            this.value = value;
        }

        int getValue() {
            return value;
        }

        @Override
        public int compareTo(Item o) {
            return Integer.compare(value, o.value);
        }

        @Override
        public boolean equals(Object obj) {
            return value == ((Item) obj).value;
        }
    }
}
