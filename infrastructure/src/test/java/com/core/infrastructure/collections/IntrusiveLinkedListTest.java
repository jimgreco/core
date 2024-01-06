package com.core.infrastructure.collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.BDDAssertions.then;

public class IntrusiveLinkedListTest {

    private IntrusiveLinkedList<IntrusiveItem> list;

    @BeforeEach
    void before_each() {
        list = new IntrusiveLinkedList<>();
    }

    @Test
    void size_is_initially_zero() {
        then(list.size()).isEqualTo(0);
        then(list.isEmpty()).isTrue();
    }

    @Test
    void insert_and_remove() {
        list.insert(new IntrusiveItem(1));

        then(list.removeFirst().getValue()).isEqualTo(1);
        then(list.isEmpty()).isTrue();
        then(list.getFirst()).isNull();
    }

    @Test
    void insert_many() {
        list.insert(new IntrusiveItem(1));
        list.insert(new IntrusiveItem(3));
        list.insert(new IntrusiveItem(-1));
        list.insert(new IntrusiveItem(4));
        list.insert(new IntrusiveItem(2));

        then(list.removeFirst().getValue()).isEqualTo(-1);
        then(list.removeFirst().getValue()).isEqualTo(1);
        then(list.removeFirst().getValue()).isEqualTo(2);
        then(list.removeFirst().getValue()).isEqualTo(3);
        then(list.removeFirst().getValue()).isEqualTo(4);
        then(list.isEmpty()).isTrue();
        then(list.getFirst()).isNull();
    }

    @Test
    void addFirst_adds_to_beginning_of_list() {
        list.addFirst(new IntrusiveItem(1));
        list.addFirst(new IntrusiveItem(2));
        list.addFirst(new IntrusiveItem(3));

        then(list.removeFirst().getValue()).isEqualTo(3);
        then(list.removeFirst().getValue()).isEqualTo(2);
        then(list.removeFirst().getValue()).isEqualTo(1);
        then(list.isEmpty()).isTrue();
    }

    @Test
    void addLast_adds_to_end_of_list() {
        list.addLast(new IntrusiveItem(1));
        list.addLast(new IntrusiveItem(2));
        list.addLast(new IntrusiveItem(3));

        then(list.removeFirst().getValue()).isEqualTo(1);
        then(list.removeFirst().getValue()).isEqualTo(2);
        then(list.removeFirst().getValue()).isEqualTo(3);
        then(list.isEmpty()).isTrue();
    }

    @Test
    void removeLast_removes_from_end_of_list() {
        list.addLast(new IntrusiveItem(1));
        list.addLast(new IntrusiveItem(2));
        list.addLast(new IntrusiveItem(3));

        then(list.removeLast().getValue()).isEqualTo(3);
        then(list.removeLast().getValue()).isEqualTo(2);
        then(list.removeLast().getValue()).isEqualTo(1);
        then(list.isEmpty()).isTrue();
    }

    @Test
    void predicate_delete_middle() {
        list.insert(new IntrusiveItem(3));
        var expected = new IntrusiveItem(1);
        list.insert(expected);
        list.insert(new IntrusiveItem(-1));
        list.insert(new IntrusiveItem(1));
        list.insert(new IntrusiveItem(4));

        var actual = list.removeFirst(intrusiveItem -> intrusiveItem.getValue() == 1);

        then(actual).isSameAs(expected);
        then(list.removeFirst().getValue()).isEqualTo(-1);
        then(list.removeFirst().getValue()).isEqualTo(1);
        then(list.removeFirst().getValue()).isEqualTo(3);
        then(list.removeFirst().getValue()).isEqualTo(4);
        then(list.isEmpty()).isTrue();
        then(list.getFirst()).isNull();
    }

    @Test
    void predicate_delete_first() {
        list.insert(new IntrusiveItem(3));
        var expected = new IntrusiveItem(-1);
        list.insert(expected);
        list.insert(new IntrusiveItem(-1));
        list.insert(new IntrusiveItem(1));
        list.insert(new IntrusiveItem(4));

        var actual = list.removeFirst(intrusiveItem -> intrusiveItem.getValue() == -1);

        then(actual).isSameAs(expected);
        then(list.removeFirst().getValue()).isEqualTo(-1);
        then(list.removeFirst().getValue()).isEqualTo(1);
        then(list.removeFirst().getValue()).isEqualTo(3);
        then(list.removeFirst().getValue()).isEqualTo(4);
        then(list.isEmpty()).isTrue();
        then(list.getFirst()).isNull();
    }

    @Test
    void predicate_delete_last() {
        list.insert(new IntrusiveItem(3));
        var expected = new IntrusiveItem(5);
        list.insert(expected);
        list.insert(new IntrusiveItem(-1));
        list.insert(new IntrusiveItem(1));
        list.insert(new IntrusiveItem(4));

        var actual = list.removeFirst(intrusiveItem -> intrusiveItem.getValue() == 5);

        then(actual).isSameAs(expected);
        then(list.removeFirst().getValue()).isEqualTo(-1);
        then(list.removeFirst().getValue()).isEqualTo(1);
        then(list.removeFirst().getValue()).isEqualTo(3);
        then(list.removeFirst().getValue()).isEqualTo(4);
        then(list.isEmpty()).isTrue();
        then(list.getFirst()).isNull();
    }

    @Test
    void getFirst_does_not_remove_from_the_beginning_of_list() {
        list.addLast(new IntrusiveItem(1));
        list.addLast(new IntrusiveItem(2));
        list.addLast(new IntrusiveItem(3));

        then(list.getFirst().getValue()).isEqualTo(1);
        then(list.getFirst().getValue()).isEqualTo(1);
    }

    @Test
    void getLast_does_not_remove_from_the_end_of_list() {
        list.addLast(new IntrusiveItem(1));
        list.addLast(new IntrusiveItem(2));
        list.addLast(new IntrusiveItem(3));

        then(list.getLast().getValue()).isEqualTo(3);
        then(list.getLast().getValue()).isEqualTo(3);
    }

    @Test
    void iterate_through_list_with_iterable() {
        list.addLast(new IntrusiveItem(1));
        list.addLast(new IntrusiveItem(2));
        list.addLast(new IntrusiveItem(3));
        var actual = new ArrayList<>();

        for (var item : list) {
            actual.add(item.getValue());
        }

        then(actual).containsExactly(1, 2, 3);
    }

    @Test
    void iterate_through_list_with_iterator() {
        list.addLast(new IntrusiveItem(1));
        list.addLast(new IntrusiveItem(2));
        list.addLast(new IntrusiveItem(3));
        var actual = new ArrayList<>();
        var iterator = list.iterator();

        actual.add(iterator.next().getValue());
        actual.add(iterator.next().getValue());
        actual.add(iterator.next().getValue());

        then(iterator.hasNext()).isFalse();
        then(actual).containsExactly(1, 2, 3);
    }

    @Test
    void contains_for_element_in_list() {
        var item1 = new IntrusiveItem(1);
        var item2 = new IntrusiveItem(2);
        var item3 = new IntrusiveItem(3);
        list.addLast(item1);
        list.addLast(item2);
        list.addLast(item3);

        then(list.contains(item1)).isTrue();
        then(list.contains(item2)).isTrue();
        then(list.contains(item3)).isTrue();
    }

    @Test
    void contains_for_null_is_false() {
        var item1 = new IntrusiveItem(1);
        var item2 = new IntrusiveItem(2);
        list.addLast(item1);
        list.addLast(item2);

        then(list.contains(null)).isFalse();
    }

    @Test
    void contains_for_unknown_element_is_false() {
        var item1 = new IntrusiveItem(1);
        var item2 = new IntrusiveItem(2);
        var item3 = new IntrusiveItem(3);
        list.addLast(item1);
        list.addLast(item2);

        then(list.contains(item3)).isFalse();
    }
}
