package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.ObjectPool;
import com.core.infrastructure.collections.Resettable;
import org.agrona.DirectBuffer;

import java.util.Arrays;
import java.util.List;

class BufferFixMsg implements FixMsg {

    private static final int DEFAULT_MAX_TAGS = 25;

    private final DirectBuffer bufferWrapper;
    private final FixDateTimeParser timestampParser;

    private int[] tags;
    private DirectBuffer[] values;
    private DirectBuffer buffer;
    private int startByte;
    private int endByte;
    private int numTags;

    private List<FixMsg> repeatingGroups;
    private ObjectPool<RepeatingGroup> repeatingGroupsPool;

    BufferFixMsg() {
        tags = new int[DEFAULT_MAX_TAGS];
        values = new DirectBuffer[DEFAULT_MAX_TAGS];
        for (var i = 0; i < values.length; i++) {
            values[i] = BufferUtils.emptyBuffer();
        }
        bufferWrapper = BufferUtils.emptyBuffer();
        timestampParser = new FixDateTimeParser();
    }

    void start(DirectBuffer buffer, int offset) {
        this.buffer = buffer;
        startByte = offset;
        endByte = offset;
        numTags = 0;
    }

    void end(int offset) {
        endByte = offset;
    }

    DirectBuffer addField(int tag, DirectBuffer value, int offset, int length) {
        checkNumFields(numTags);

        tags[numTags] = tag;
        var wrappedValue = values[numTags];
        wrappedValue.wrap(value, offset, length);

        numTags++;
        endByte = offset + length;
        return wrappedValue;
    }

    private void checkNumFields(int tagCount) {
        if (tagCount == tags.length) {
            tags = Arrays.copyOf(tags, 2 * tags.length);
            var originalLength = values.length;
            values = Arrays.copyOf(values, 2 * values.length);
            for (var i = originalLength; i < values.length; i++) {
                values[i] = BufferUtils.emptyBuffer();
            }
        }
    }

    void parse(DirectBuffer buffer, int offset, int length) {
        var tag = 0;
        var inTag = true;
        var tagCount = 0;
        var valueStart = -1;

        for (var i = offset; i < offset + length; i++) {
            var theByte = buffer.getByte(i);
            if (theByte == '=') {
                valueStart = i + 1;
                inTag = false;
            } else if (theByte == FixUtils.SOH) {
                checkNumFields(tagCount);
                tags[tagCount] = tag;
                values[tagCount].wrap(buffer, valueStart, i - valueStart);
                tagCount++;
                tag = 0;
                inTag = true;
            } else if (inTag) {
                tag *= 10;
                tag += theByte - '0';
            }
        }

        this.buffer = buffer;
        this.startByte = offset;
        this.endByte = offset + length;
        numTags = tagCount;
    }

    @Override
    public DirectBuffer getBuffer() {
        bufferWrapper.wrap(buffer, startByte, endByte - startByte);
        return bufferWrapper;
    }

    @Override
    public int getSize() {
        return numTags;
    }

    @Override
    public int getTagAt(int index) {
        checkIndex(index);
        return tags[index];
    }

    @Override
    public DirectBuffer getValueAt(int index) {
        checkIndex(index);
        return values[index];
    }

    @Override
    public long getValueAsTimestamp(int tag) {
        var value = getValue(tag);
        return value == null ? -1 : timestampParser.parseDateTime(value, 0, value.capacity());
    }

    @Override
    public long getValueAsTimestampAt(int index) {
        var value = getValueAt(index);
        return value == null ? -1 : timestampParser.parseDateTime(value, 0, value.capacity());
    }

    @Override
    public List<FixMsg> getRepeatingGroups(int numGroupsTag, int firstTagInGroup) {
        if (repeatingGroupsPool == null) {
            // initialize repeating groups
            repeatingGroups = new CoreList<>();
            repeatingGroupsPool = new ObjectPool<>(RepeatingGroup::new, 10);
        }

        // return repeating groups to the
        for (var i = repeatingGroups.size() - 1; i >= 0; i--) {
            var group = repeatingGroups.remove(i);
            repeatingGroupsPool.returnObject((RepeatingGroup) group);
        }

        var tagIndex = 0;
        var numRepeatingGroups = -1L;

        while (numRepeatingGroups == -1L && tagIndex < numTags) {
            var tag = tags[tagIndex];
            if (tag == numGroupsTag) {
                numRepeatingGroups = getValueAsIntegerAt(tagIndex, -2);
            }
            tagIndex++;
        }

        if (numRepeatingGroups <= 0 || tagIndex >= numTags || tags[tagIndex] != firstTagInGroup) {
            return repeatingGroups;
        }

        // first group
        var startOfGroup = tagIndex;
        var repeatingGroup = repeatingGroupsPool.borrowObject();
        repeatingGroup.start(startOfGroup);
        repeatingGroups.add(repeatingGroup);
        tagIndex++;

        while (repeatingGroups.size() < numRepeatingGroups && tagIndex < numTags) {
            var tag = tags[tagIndex];
            if (tag == firstTagInGroup) {
                // this is the start of the next group, end the last group
                repeatingGroup.end(tagIndex);

                // create a new group
                startOfGroup = tagIndex;
                repeatingGroup = repeatingGroupsPool.borrowObject();
                repeatingGroup.start(startOfGroup);
                repeatingGroups.add(repeatingGroup);
            }
            tagIndex++;
        }

        return repeatingGroups;
    }

    @Override
    public String toString() {
        return FixUtils.toFixString(getBuffer());
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= getSize()) {
            throw new IndexOutOfBoundsException(
                    "invalid tag index: index=" + index + ", numTags=" + getSize());
        }
    }

    private class RepeatingGroup implements FixMsg, Resettable {

        private final DirectBuffer wrapper;
        private int tagsOffset;
        private int tagsLength;

        RepeatingGroup() {
            wrapper = BufferUtils.emptyBuffer();
        }

        @Override
        public DirectBuffer getBuffer() {
            var tagIndex = 0;
            var groupStartByte = 0;
            var groupEndByte = 0;

            for (var i = startByte; i < endByte; i++) {
                if (buffer.getByte(i) == FixUtils.SOH) {
                    if (tagIndex == tagsOffset) {
                        groupStartByte = i;
                    } else if (tagIndex == tagsOffset + tagsLength) {
                        groupEndByte = i;
                    }
                }
            }

            wrapper.wrap(buffer, groupStartByte, groupEndByte);
            return wrapper;
        }

        @Override
        public int getSize() {
            return tagsLength;
        }

        @Override
        public int getTagAt(int index) {
            checkGroupIndex(index);
            return BufferFixMsg.this.getTagAt(tagsOffset + index);
        }

        @Override
        public DirectBuffer getValueAt(int index) {
            checkGroupIndex(index);
            return BufferFixMsg.this.getValueAt(tagsOffset + index);
        }

        @Override
        public long getValueAsTimestampAt(int index) {
            checkGroupIndex(index);
            return BufferFixMsg.this.getValueAsTimestampAt(tagsOffset + index);
        }

        @Override
        public long getValueAsTimestamp(int tag) {
            var value = getValue(tag);
            return value == null ? -1 : timestampParser.parseDateTime(value, 0, value.capacity());
        }

        @Override
        public List<FixMsg> getRepeatingGroups(int numGroupsTag, int firstTagInGroup) {
            throw new UnsupportedOperationException("nested repeating groups are not supported");
        }

        @Override
        public void reset() {
            tagsOffset = -1;
            tagsLength = -1;
        }

        @Override
        public String toString() {
            return FixUtils.toFixString(getBuffer());
        }

        void start(int groupStart) {
            // the length is the entire rest of the FIX message because we don't specify specific tags in the group
            tagsOffset = groupStart;
            end(numTags);
        }

        void end(int groupEnd) {
            tagsLength = groupEnd - tagsOffset;
        }

        private void checkGroupIndex(int index) {
            if (index < 0 || index >= getSize()) {
                throw new IndexOutOfBoundsException(
                        "invalid tag index: index=" + index + ", numTags=" + getSize());
            }
        }
    }
}
