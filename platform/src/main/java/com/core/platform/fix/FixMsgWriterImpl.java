package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.buffer.BufferNumberUtils;
import com.core.infrastructure.time.Time;
import com.core.platform.fix.schema.Fix42;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.function.Consumer;

class FixMsgWriterImpl implements FixMsgWriter {

    private static final int MAX_CACHED_TAG = 117;
    private static final DirectBuffer BODYLENGTH_DEFAULT = BufferUtils.allocate(4);

    private final Time time;
    private final FixTimestampFormatter timestampFormatter;
    private final Consumer<DirectBuffer> commitListener;

    private final MutableDirectBuffer buffer;
    private final DirectBuffer messageWrapper;

    private final DirectBuffer[] cachedTags;
    private final int[] cachedTagChecksums;

    private final FixSessionConfiguration sessionConfiguration;

    private int bodyLengthValuePosition;
    private int msgTypeTagPosition;

    private int position;
    private int checksum;
    private BufferFixMsg fixMsg;

    FixMsgWriterImpl(Time time, FixSessionConfiguration sessionConfiguration, Consumer<DirectBuffer> commitListener) {
        this.time = time;
        this.sessionConfiguration = sessionConfiguration;
        this.commitListener = commitListener;

        timestampFormatter = new FixTimestampFormatter();
        cachedTags = new DirectBuffer[MAX_CACHED_TAG + 1];
        cachedTagChecksums = new int[MAX_CACHED_TAG + 1];
        buffer = BufferUtils.allocate(4096);
        messageWrapper = BufferUtils.emptyBuffer();
    }

    FixMsgWriter start(DirectBuffer msgType, int msgSeqNum) {
        position = 0;
        checksum = 0;

        putBuffer(Fix42.BEGIN_STRING, sessionConfiguration.getFixVersion().getBeginString());
        bodyLengthValuePosition = position + "9=".length();
        putBuffer(Fix42.BODY_LENGTH, BODYLENGTH_DEFAULT);
        msgTypeTagPosition = position;
        putBuffer(Fix42.MSG_TYPE, msgType);
        putBuffer(Fix42.SENDER_COMP_ID, sessionConfiguration.getSenderCompIdAsBuffer());
        putBuffer(Fix42.TARGET_COMP_ID, sessionConfiguration.getTargetCompIdAsBuffer());
        putInteger(Fix42.MSG_SEQ_NUM, msgSeqNum);
        putTimestamp(Fix42.SENDING_TIME, time.nanos());

        return this;
    }

    @Override
    public DirectBuffer getBuffer() {
        messageWrapper.wrap(buffer, 0, position);
        return messageWrapper;
    }

    @Override
    public void send() {
        // write BodyLength[9] value
        var bodyLength = position - msgTypeTagPosition;
        buffer.putNaturalPaddedIntAscii(bodyLengthValuePosition, BODYLENGTH_DEFAULT.capacity(), bodyLength);
        for (var i = bodyLengthValuePosition; i < bodyLengthValuePosition + BODYLENGTH_DEFAULT.capacity(); i++) {
            checksum += buffer.getByte(i);
        }

        // write Checksum[10]
        var threeDigitChecksum = checksum % 256;
        writeTag(Fix42.CHECK_SUM);
        buffer.putNaturalPaddedIntAscii(position, 3, threeDigitChecksum);
        position += 3;
        writeSoh();

        messageWrapper.wrap(buffer, 0, position);
        commitListener.accept(messageWrapper);
    }

    @Override
    public FixMsg getFixMsg() {
        if (fixMsg == null) {
            fixMsg = new BufferFixMsg();
        }
        fixMsg.parse(buffer, 0, position);
        return fixMsg;
    }

    @Override
    public FixMsgWriterImpl putBuffer(int tag, DirectBuffer value, int offset, int length) {
        writeTag(tag);
        for (var i = offset; i < offset + length; i++) {
            var theByte = value.getByte(i);
            buffer.putByte(position++, theByte);
            checksum += theByte;
        }
        writeSoh();
        return this;
    }

    @Override
    public FixMsgWriter putAsciiString(int tag, String value) {
        writeTag(tag);
        for (var i = 0; i < value.length(); i++) {
            var theByte = value.charAt(i);
            buffer.putByte(position++, (byte) theByte);
            checksum += theByte;
        }
        writeSoh();
        return this;
    }

    @Override
    public FixMsgWriter putAsciiCharacter(int tag, char value) {
        writeTag(tag);
        buffer.putByte(position++, (byte) value);
        checksum += value;
        writeSoh();
        return this;
    }

    @Override
    public FixMsgWriterImpl putInteger(int tag, long value) {
        writeTag(tag);
        var start = position;
        position += buffer.putLongAscii(position, value);
        updateChecksum(start);
        writeSoh();
        return this;
    }

    @Override
    public FixMsgWriter putDecimal(int tag, double value) {
        writeTag(tag);
        var start = position;
        position += BufferNumberUtils.putAsAsciiDecimal(buffer, position, value);
        updateChecksum(start);
        writeSoh();
        return this;
    }

    @Override
    public FixMsgWriterImpl putTimestamp(int tag, long value) {
        writeTag(tag);
        var start = position;
        position += timestampFormatter.writeDateTime(buffer, position, time.nanos());
        updateChecksum(start);
        writeSoh();
        return this;
    }

    @Override
    public String toString() {
        return FixUtils.toFixString(buffer, 0, position);
    }

    private void writeTag(int tag) {
        if (tag < cachedTags.length) {
            var cachedTag = cachedTags[tag];
            if (cachedTag == null) {
                cachedTag = BufferUtils.fromAsciiString(tag + "=");
                cachedTags[tag] = cachedTag;
                for (var i = 0; i < cachedTag.capacity(); i++) {
                    cachedTagChecksums[tag] += cachedTag.getByte(i);
                }
            }
            buffer.putBytes(position, cachedTag, 0, cachedTag.capacity());
            position += cachedTag.capacity();
            checksum += cachedTagChecksums[tag];
        } else {
            var start = position;
            position += buffer.putIntAscii(position, tag);
            buffer.putByte(position++, (byte) '=');
            updateChecksum(start);
        }
    }

    private void writeSoh() {
        buffer.putByte(position++, FixUtils.SOH);
        checksum++;
    }

    private void updateChecksum(int start) {
        for (var i = start; i < position; i++) {
            checksum += buffer.getByte(i);
        }
    }
}
