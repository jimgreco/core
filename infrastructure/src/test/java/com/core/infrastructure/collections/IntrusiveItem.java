package com.core.infrastructure.collections;

class IntrusiveItem implements IntrusiveLinkedList.IntrusiveLinkedListItem<IntrusiveItem> {

    private IntrusiveLinkedList.IntrusiveLinkedListItem<IntrusiveItem> prev;
    private IntrusiveLinkedList.IntrusiveLinkedListItem<IntrusiveItem> next;
    private final int value;

    IntrusiveItem(int value) {
        this.value = value;
    }

    @Override
    public IntrusiveLinkedList.IntrusiveLinkedListItem<IntrusiveItem> getPrevious() {
        return prev;
    }

    @Override
    public IntrusiveLinkedList.IntrusiveLinkedListItem<IntrusiveItem> getNext() {
        return next;
    }

    @Override
    public void setPrevious(IntrusiveLinkedList.IntrusiveLinkedListItem<IntrusiveItem> prev) {
        this.prev = prev;
    }

    @Override
    public void setNext(IntrusiveLinkedList.IntrusiveLinkedListItem<IntrusiveItem> next) {
        this.next = next;
    }

    @Override
    public IntrusiveItem getItem() {
        return this;
    }

    @Override
    public int compareTo(IntrusiveItem o) {
        return Integer.compare(value, o.value);
    }

    public int getValue() {
        return value;
    }
}
