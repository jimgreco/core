package com.core.infrastructure.collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;

public class ObjectPoolTest {

    private ObjectPool<Object> pool;

    @BeforeEach
    void before_each() {
        pool = new ObjectPool<>(Object::new, 5);
    }

    @Test
    void initial_size_is_specified() {
        then(pool.getSize()).isEqualTo(5);
        then(pool.getOutstanding()).isEqualTo(0);
        then(pool.getRemaining()).isEqualTo(5);
    }

    @Test
    void remove_from_pool_changes_size() {
        pool.borrowObject();

        pool.borrowObject();

        then(pool.getSize()).isEqualTo(5);
        then(pool.getOutstanding()).isEqualTo(2);
        then(pool.getRemaining()).isEqualTo(3);
    }

    @Test
    void grow_pool() {
        for (var i = 0; i < 6; i++) {
            pool.borrowObject();
        }

        then(pool.getSize()).isEqualTo(10);
        then(pool.getOutstanding()).isEqualTo(6);
        then(pool.getRemaining()).isEqualTo(4);
    }

    @Test
    void return_item_to_pool() {
        var item = pool.borrowObject();

        pool.returnObject(item);

        then(pool.getSize()).isEqualTo(5);
        then(pool.getOutstanding()).isEqualTo(0);
        then(pool.getRemaining()).isEqualTo(5);
    }

    @Test
    void throw_exception_returning_item_to_pool() {
        var item = pool.borrowObject();
        pool.returnObject(item);

        thenThrownBy(() -> pool.returnObject(item)).isInstanceOf(IllegalStateException.class);
    }
}
