package com.core.platform.bus.mold;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.SeekableBufferChannel;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;

/**
 * A {@code MessageStore} that uses two {@code SeekableBufferChannel}s to store messages.
 *
 * <p>The first file is the message file which stores the raw bytes of the message with a 2-byte
 * header that stores the length.
 * <table>
 *     <caption>Message file format</caption>
 *     <tr>
 *         <th>Offset</th>
 *         <th>Length</th>
 *         <th>Description</th>
 *     </tr>
 *     <tr>
 *         <td>0</td>
 *         <td>2</td>
 *         <td>The length of the message field, N</td>
 *     </tr>
 *     <tr>
 *         <td>2</td>
 *         <td>N</td>
 *         <td>The message contents</td>
 *     </tr>
 * </table>
 *
 * <p>Messages are stored subsequently in the file with no gaps in between.
 * In this way, even without the index file, all messages can be processed by reading the messages
 * one by one from the beginning of the file.
 *
 * <p>The second file is the index file which stores the offset into the messages file of the
 * message corresponding to its position in the index file.
 * Each offset is stored as a long.
 * To read the Nth message, first read a long from the index file at {@code 8 * (N - 1)} to get the
 * offset into the messages file where the message is stored.
 * Then read a long from the index file at {@code 8 * N} to get the offset into the message file
 * where the first byte of the next message is stored.
 * Then read the message from the messages file between the indexes.
 */
public abstract class ChannelMessageStore implements MessageStore, Encodable {

    private final MutableDirectBuffer messageWriteBuffer;
    private final MutableDirectBuffer indexBuffer;
    private long numMessages;
    private SeekableBufferChannel indexFile;
    private SeekableBufferChannel messagesFile;
    private long messagesFileWritePosition;
    private long indexFileWritePosition;

    /**
     * Creates an empty message store.
     */
    protected ChannelMessageStore() {
        messageWriteBuffer = BufferUtils.allocateDirect(MoldConstants.MTU_SIZE - MoldConstants.HEADER_SIZE);
        indexBuffer = BufferUtils.allocateDirect(2 * Long.BYTES);
    }

    /**
     * Creates a buffer channel to store the index file for a session with the specified name.
     *
     * @param sessionName the name of the session
     * @return the buffer channel
     * @throws IOException if an I/O error occurs
     */
    protected abstract SeekableBufferChannel createIndexFile(DirectBuffer sessionName) throws IOException;

    /**
     * Creates a buffer channel to store the message file for a session with the specified name.
     *
     * @param sessionName the name of the session
     * @return the buffer channel
     * @throws IOException if an I/O error occurs
     */
    protected abstract SeekableBufferChannel createMessageFile(DirectBuffer sessionName) throws IOException;

    @Override
    public void open(DirectBuffer sessionName) throws IOException {
        this.messagesFile = createMessageFile(sessionName);
        this.indexFile = createIndexFile(sessionName);
    }

    @Override
    public void close() throws IOException {
        indexFile.close();
        messagesFile.close();
        numMessages = 0;
    }

    @Override
    public MutableDirectBuffer acquire() {
        return messageWriteBuffer;
    }

    @Override
    public void commit(int[] messageLengths, int index, int length) throws IOException {
        var position = 0;
        for (var i = 0; i < length; i++) {
            var messageLength = messageLengths[index + i];
            indexBuffer.putLong(0, messagesFileWritePosition);
            messagesFileWritePosition += messagesFile.write(
                    messagesFileWritePosition, messageWriteBuffer, position, messageLength);
            indexFileWritePosition += indexFile.write(indexFileWritePosition, indexBuffer, 0, Long.BYTES);
            position += messageLength;
            numMessages++;
        }
    }

    @Override
    public int read(MutableDirectBuffer buffer, int index, long seqNum)
            throws IOException {
        if (seqNum <= 0 || seqNum > numMessages) {
            throw new IllegalArgumentException(
                    "invalid message requested: seqNum=" + seqNum
                            + ", numMessages=" + numMessages);
        }

        // start position of the message
        var indexFilePosition = Long.BYTES * (seqNum - 1);
        var endOfFile = seqNum == numMessages;
        var bytesToRead = endOfFile ? 8 : 16;
        var bytesRead = indexFile.read(indexFilePosition, indexBuffer, 0, bytesToRead);
        var msgStartPosition = indexBuffer.getLong(0);
        var msgEndPosition = endOfFile ? messagesFile.size() : indexBuffer.getLong(Long.BYTES);
        if (bytesToRead != bytesRead) {
            throw new IOException("could not read index file");
        }

        // read messages file
        var messageLength = (int) (msgEndPosition - msgStartPosition);
        bytesRead = messagesFile.read(msgStartPosition, buffer, index, messageLength);
        if (bytesRead != messageLength) {
            throw new IOException("could not read messages file");
        }

        return messageLength;
    }

    @Override
    public long getNumMessages() {
        return numMessages;
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    @Command(path = "status", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("type").string(getClass().getSimpleName())
                .string("numMessages").number(numMessages)
                .string("indexFile").object(indexFile)
                .string("indexFilePosition").number(indexFileWritePosition)
                .string("messagesFile").object(messagesFile)
                .string("messagesFilePosition").number(messagesFileWritePosition)
                .closeMap();
    }
}
