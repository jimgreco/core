package com.core.infrastructure.io;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Preferred;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.util.Objects;
import java.util.Set;

/**
 * A channel for reading, writing, mapping, and manipulating a file.
 *
 * <p>A file channel is a {@link SeekableBufferChannel} that is connected to a file.
 * It has a current <i>position</i> within its file which can be both {@link #position() <i>queried</i>} and
 * {@link #position(long) <i>modified</i>}.
 * The file itself contains a variable-length sequence of bytes that can be read and written and whose current
 * {@link #size <i>size</i>} can be queried.
 * The size of the file increases when bytes are written beyond its current size; the size of the file decreases when it
 * is {@link #truncate <i>truncated</i>}.
 * The file may also have some associated <i>metadata</i> such as access permissions, content type, and
 * last-modification time; this class does not define methods for metadata access.
 *
 * <p>In addition to the familiar read, write, and close operations of byte channels, this class defines the following
 * file-specific operations:
 *
 * <ul>
 *   <li>Bytes may be {@link #read(long, MutableDirectBuffer, int, int) read} or
 *       {@link #write(long, DirectBuffer, int, int) <i>written</i>} at an absolute position in a file in a way that
 *       does not affect the channel's current position.
 *   <li>... TODO: other methods
 * </ul>
 *
 * <p>The view of a file provided by an instance of this class is guaranteed to be consistent with other views of the
 * same file provided by other instances in the same program.
 * The view provided by an instance of this class may or may not, however, be consistent with the views seen by other
 * concurrently-running programs due to caching performed by the underlying operating system and delays induced by
 * network-filesystem protocols.
 * This is true regardless of the language in which these other programs are written, and whether they are running on
 * the same machine or on some other machine.
 * The exact nature of any such inconsistencies are system-dependent and are therefore unspecified.
 */
public class FileChannel implements SeekableBufferChannel, Encodable {

    private final java.nio.channels.FileChannel channel;
    private Runnable readListener;
    private Runnable writeListener;

    /**
     * Opens or creates a file.
     *
     * <p>An invocation of this method behaves in exactly the same way as the invocation
     * <pre>
     *     var fc = new {@link #FileChannel(Path, Set, FileAttribute[]) FileChannel}(
     *          file, opts, new FileAttribute&lt;?&gt;[0]);
     * </pre>
     * where {@code opts} is a set of the options specified in the {@code
     * options} array.
     *
     * @param path the path of the file to open or create
     * @param options options specifying how the file is opened
     * @throws IllegalArgumentException iIf the set contains an invalid combination of options
     * @throws UnsupportedOperationException if the {@code path} is associated with a provider that does not support
     *     creating file channels, or an unsupported open option is specified
     * @throws IOException if an I/O error occurs
     * @throws SecurityException If a security manager is installed and it denies an unspecified permission required by
     *     the implementation.
     *     In the case of the default provider, the {@link SecurityManager#checkRead(String)} method is invoked to check
     *     read access if the file is opened for reading.
     *     The {@link SecurityManager#checkWrite(String)} method is invoked to check write access if the file is opened
     *     for writing
     */
    @Preferred
    public FileChannel(Path path, OpenOption... options) throws IOException {
        this(java.nio.channels.FileChannel.open(path, options));
    }

    /**
     * Opens or creates a file.
     *
     * <p>The {@code options} parameter determines how the file is opened.
     * The {@link StandardOpenOption#READ READ} and {@link StandardOpenOption#WRITE WRITE} options determine if the file
     * should be opened for reading and/or writing.
     * If neither option (or the {@link StandardOpenOption#APPEND APPEND} option) is contained in the array then the
     * file is opened for reading.
     * By default reading or writing commences at the beginning of the file.
     *
     * <p>In the addition to {@code READ} and {@code WRITE}, the following options may be present:
     *
     * <table class="striped">
     * <caption style="display:none">additional options</caption>
     * <thead>
     * <tr> <th scope="col">Option</th> <th scope="col">Description</th> </tr>
     * </thead>
     * <tbody>
     * <tr>
     *   <th scope="row"> {@link StandardOpenOption#APPEND APPEND} </th>
     *   <td> If this option is present then the file is opened for writing and
     *   each invocation of the channel's {@code write} method first advances
     *   the position to the end of the file and then writes the requested
     *   data. Whether the advancement of the position and the writing of the
     *   data are done in a single atomic operation is system-dependent and
     *   therefore unspecified. This option may not be used in conjunction
     *   with the {@code READ} or {@code TRUNCATE_EXISTING} options. </td>
     * </tr>
     * <tr>
     *   <th scope="row"> {@link StandardOpenOption#TRUNCATE_EXISTING TRUNCATE_EXISTING} </th>
     *   <td> If this option is present then the existing file is truncated to
     *   a size of 0 bytes. This option is ignored when the file is opened only
     *   for reading. </td>
     * </tr>
     * <tr>
     *   <th scope="row"> {@link StandardOpenOption#CREATE_NEW CREATE_NEW} </th>
     *   <td> If this option is present then a new file is created, failing if
     *   the file already exists. When creating a file the check for the
     *   existence of the file and the creation of the file if it does not exist
     *   is atomic with respect to other file system operations. This option is
     *   ignored when the file is opened only for reading. </td>
     * </tr>
     * <tr>
     *   <th scope="row" > {@link StandardOpenOption#CREATE CREATE} </th>
     *   <td> If this option is present then an existing file is opened if it
     *   exists, otherwise a new file is created. When creating a file the check
     *   for the existence of the file and the creation of the file if it does
     *   not exist is atomic with respect to other file system operations. This
     *   option is ignored if the {@code CREATE_NEW} option is also present or
     *   the file is opened only for reading. </td>
     * </tr>
     * <tr>
     *   <th scope="row" > {@link StandardOpenOption#DELETE_ON_CLOSE DELETE_ON_CLOSE} </th>
     *   <td> When this option is present then the implementation makes a
     *   <em>best effort</em> attempt to delete the file when closed by
     *   the {@link #close close} method. If the {@code close} method is not
     *   invoked then a <em>best effort</em> attempt is made to delete the file
     *   when the Java virtual machine terminates. </td>
     * </tr>
     * <tr>
     *   <th scope="row">{@link StandardOpenOption#SPARSE SPARSE} </th>
     *   <td> When creating a new file this option is a <em>hint</em> that the
     *   new file will be sparse. This option is ignored when not creating
     *   a new file. </td>
     * </tr>
     * <tr>
     *   <th scope="row"> {@link StandardOpenOption#SYNC SYNC} </th>
     *   <td> Requires that every update to the file's content or metadata be
     *   written synchronously to the underlying storage device. (see <a
     *   href="../file/package-summary.html#integrity"> Synchronized I/O file
     *   integrity</a>). </td>
     * </tr>
     * <tr>
     *   <th scope="row"> {@link StandardOpenOption#DSYNC DSYNC} </th>
     *   <td> Requires that every update to the file's content be written
     *   synchronously to the underlying storage device. (see <a
     *   href="../file/package-summary.html#integrity"> Synchronized I/O file
     *   integrity</a>). </td>
     * </tr>
     * </tbody>
     * </table>
     *
     * <p>The {@code attrs} parameter is an optional array of file {@link FileAttribute file-attributes} to set
     * atomically when creating the file.
     *
     * <p>The new channel is created by invoking the {@link FileSystemProvider#newFileChannel newFileChannel} method on
     * the provider that created the {@code Path}.
     *
     * @param path the path of the file to open or create
     * @param options options specifying how the file is opened
     * @param attrs an optional list of file attributes to set atomically when creating the file
     * @throws IllegalArgumentException if the set contains an invalid combination of options
     * @throws UnsupportedOperationException if the {@code path} is associated with a provider that does not support
     *     creating file channels, or an unsupported open option is specified, or the array contains an attribute that
     *     cannot be set atomically when creating the file
     * @throws IOException if an I/O error occurs
     * @throws SecurityException If a security manager is installed and it denies an unspecified permission required by
     *     the implementation.
     *     In the case of the default provider, the {@link SecurityManager#checkRead(String)} method is invoked to check
     *     read access if the file is opened for reading.
     *     The {@link SecurityManager#checkWrite(String)} method is invoked to check write access if the file is opened
     *     for writing
     */
    public FileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        this(java.nio.channels.FileChannel.open(path, options, attrs));
    }

    /**
     * Creates a {@code FileChannel} from the NIO {@code FileChannel}.
     *
     * @param channel the NIO file channel
     */
    public FileChannel(java.nio.channels.FileChannel channel) {
        this.channel = Objects.requireNonNull(channel);
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public int read(long filePosition, MutableDirectBuffer buffer, int index, int length) throws IOException {
        var srcBuffer = BufferUtils.byteBuffer(buffer, index, length);
        return channel.read(srcBuffer, filePosition);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return channel.read(dsts, offset, length);
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return channel.read(dsts);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    @Override
    public int write(DirectBuffer buffer, int index, int length) throws IOException {
        var srcBuffer = BufferUtils.byteBuffer(buffer, index, length);
        return channel.write(srcBuffer);
    }

    @Override
    public int write(long filePosition, DirectBuffer buffer, int index, int length) throws IOException {
        var srcBuffer = BufferUtils.byteBuffer(buffer, index, length);
        return channel.write(srcBuffer, filePosition);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return channel.write(srcs, offset, length);
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return channel.write(srcs);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return channel.write(src);
    }

    @Override
    public void setReadListener(Runnable listener) {
        this.readListener = listener;
        while (readListener != null) {
            readListener.run();
        }
        readListener = null;
    }

    @Override
    public void setWriteListener(Runnable listener) {
        this.writeListener = listener;
        while (writeListener != null) {
            writeListener.run();
        }
        writeListener = null;
    }

    @Override
    public long position() throws IOException {
        return channel.position();
    }

    @Override
    public FileChannel position(long position) throws IOException {
        channel.position(position);
        return this;
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        channel.truncate(size);
        return this;
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
                .string("open").bool(channel.isOpen());

        if (channel.isOpen()) {
            encoder.string("position");
            try {
                encoder.number(channel.position());
            } catch (IOException e) {
                encoder.number(-1);
            }
            encoder.string("size");
            try {
                encoder.number(channel.size());
            } catch (IOException e) {
                encoder.number(-1);
            }
        }

        encoder.closeMap();
    }
}
