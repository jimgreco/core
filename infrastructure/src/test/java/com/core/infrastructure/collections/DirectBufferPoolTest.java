package com.core.infrastructure.collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

public class DirectBufferPoolTest {

    @Test
    void maxBufferCapacity_is_specified_in_the_constructor() {
        var pool = new DirectBufferPool(10, false);

        then(pool.getMaxBufferCapacity()).isEqualTo(10);
    }

    @Test
    void borrow_changes_stats() {
        var pool = new DirectBufferPool(10, false);
        pool.borrowBuffer(1);
        var item2 = pool.borrowBuffer(2);
        pool.borrowBuffer(9);

        pool.returnBuffer(item2);

        then(pool.getSize()).isEqualTo(3);
        then(pool.getOutstanding()).isEqualTo(2);
        then(pool.getRemaining()).isEqualTo(1);
    }

    @Test
    void borrow_multiple_of_same_capacity() {
        var pool = new DirectBufferPool(10, false);
        pool.borrowBuffer(10);
        pool.borrowBuffer(10);

        pool.borrowBuffer(10);

        then(pool.getSize()).isEqualTo(3);
        then(pool.getOutstanding()).isEqualTo(3);
        then(pool.getRemaining()).isEqualTo(0);
    }

    @Test
    void returned_buffers_are_returned_first() {
        var pool = new DirectBufferPool(10, false);
        var item1 = pool.borrowBuffer(10);
        var item2 = pool.borrowBuffer(10);
        pool.borrowBuffer(10);
        pool.returnBuffer(item1);
        pool.returnBuffer(item2);

        var item2b = pool.borrowBuffer(10);

        then(item2).isSameAs(item2b);
    }

    @Test
    void borrow_and_return_with_different_capacity_returns_new_buffer() {
        var pool = new DirectBufferPool(10, false);
        var item1 = pool.borrowBuffer(10);
        pool.borrowBuffer(10);

        var item3 = pool.returnAndBorrowBuffer(item1, 5);

        then(item3).isNotSameAs(item1);
        then(item3.capacity()).isEqualTo(5);
    }

    @Test
    void borrow_and_return_with_same_capacity_returns_same_buffer() {
        var pool = new DirectBufferPool(10, false);
        var item1 = pool.borrowBuffer(10);
        pool.borrowBuffer(10);

        var item3 = pool.returnAndBorrowBuffer(item1, 10);

        then(item3).isSameAs(item1);
        then(item3.capacity()).isEqualTo(10);
    }

    @Nested
    class DirectByteBuffersTests {

        private DirectBufferPool pool;

        @BeforeEach
        void before_each() {
            pool = new DirectBufferPool(10, true);
        }

        @Test
        void direct_true_is_specified_in_the_constructor() {
            then(pool.isDirect()).isTrue();
        }

        @Test
        void direct_true_allocates_direct_byte_buffers() {
            then(pool.borrowBuffer(10).byteBuffer().isDirect()).isTrue();
        }

        @Test
        void buffer_of_specified_capacity_is_borrowed() {
            then(pool.borrowBuffer(5).capacity()).isEqualTo(5);
        }
    }

    @Nested
    class HeapByteBufferTests {

        private DirectBufferPool pool;

        @BeforeEach
        void before_each() {
            pool = new DirectBufferPool(10, false);
        }

        @Test
        void direct_false_is_specified_in_the_constructor() {
            then(pool.isDirect()).isFalse();
        }

        @Test
        void direct_false_allocates_direct_byte_buffers() {
            then(pool.borrowBuffer(10).byteBuffer().isDirect()).isFalse();
        }

        @Test
        void buffer_of_specified_capacity_is_borrowed() {
            then(pool.borrowBuffer(5).capacity()).isEqualTo(5);
        }
    }
}
