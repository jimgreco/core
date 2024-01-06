package com.core.infrastructure.collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;

public class UnmodifiableListTest {

    private UnmodifiableList<String> list;

    @BeforeEach
    void before_each() {
        list = new UnmodifiableList<>();
        list.getUnderlyingList().add("A");
        list.getUnderlyingList().add("B");
        list.getUnderlyingList().add("C");
    }

    @Test
    void random_access_to_underlying_list() {
        then(list.get(0)).isEqualTo("A");
        then(list.get(1)).isEqualTo("B");
        then(list.get(2)).isEqualTo("C");
        then(list.size()).isEqualTo(3);
    }

    @Test
    void list_cannot_be_modified() {
        thenThrownBy(() -> list.add("D")).isInstanceOf(UnsupportedOperationException.class);
        thenThrownBy(() -> list.set(1, "D")).isInstanceOf(UnsupportedOperationException.class);
        thenThrownBy(() -> list.add(0, "D")).isInstanceOf(UnsupportedOperationException.class);
    }
}
