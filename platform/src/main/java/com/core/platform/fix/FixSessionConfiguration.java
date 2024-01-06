package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import org.agrona.DirectBuffer;

import java.util.Objects;

/**
 * The FIX session configuration.
 */
public class FixSessionConfiguration implements Encodable {

    private FixVersion fixVersion;
    private String senderCompId;
    private String targetCompId;
    private String username;
    private String password;
    private String account;
    private int heartbeatInterval;
    private DirectBuffer senderCompIdAsBuffer;
    private DirectBuffer targetCompIdAsBuffer;

    /**
     * Constructs an empty {@code FixSessionConfiguration} with the FIX version set to FIX 4.2.
     */
    public FixSessionConfiguration() {
        fixVersion = FixVersion.FIX42;
        heartbeatInterval = FixUtils.DEFAULT_HEART_BT_INT;
    }

    /**
     * Constructs a {@code FixSessionConfiguration} with the specified session parameters.
     *
     * @param fixVersion the FIX version
     * @param senderCompId the SenderCompID[49] value
     * @param targetCompId the TargetCompID[56] value
     */
    public FixSessionConfiguration(FixVersion fixVersion, String senderCompId, String targetCompId) {
        setFixVersion(fixVersion);
        setSenderCompId(senderCompId);
        setTargetCompId(targetCompId);
    }

    /**
     * Sets the session's FIX version.
     *
     * @param fixVersion the session's FIX version
     */
    @Command
    public void setFixVersion(FixVersion fixVersion) {
        this.fixVersion = Objects.requireNonNull(fixVersion, "fixVersion is null");
    }

    /**
     * Sets the SenderCompID[49] value.
     *
     * @param senderCompId the SenderCompID[49] value
     */
    @Command
    public void setSenderCompId(String senderCompId) {
        if (!senderCompId.equals(this.senderCompId)) {
            this.senderCompId = senderCompId;
            senderCompIdAsBuffer = BufferUtils.fromAsciiString(senderCompId);
        }
    }

    /**
     * Sets the TargetCompID[56] value.
     *
     * @param targetCompId the TargetCompID[56] value
     */
    @Command
    public void setTargetCompId(String targetCompId) {
        if (!targetCompId.equals(this.targetCompId)) {
            this.targetCompId = targetCompId;
            targetCompIdAsBuffer = BufferUtils.fromAsciiString(targetCompId);
        }
    }

    /**
     * Sets the Username[553] value.
     *
     * @param username the Username[553] value
     */
    @Command
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Sets the Password[554] value.
     *
     * @param password the Password[554] value
     */
    @Command
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Sets the HeartBtInt[108] value in seconds.
     *
     * @param heartbeatInterval the HeartBtInt[108] value in seconds
     */
    @Command
    public void setHeartbeatInterval(int heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    /**
     * Sets the Account[1] value.
     *
     * @param account the Account[1] value
     */
    @Command
    public void setAccount(String account) {
        this.account = account;
    }

    /**
     * Returns the session's FIX version.
     *
     * @return the session's FIX version
     */
    public FixVersion getFixVersion() {
        return fixVersion;
    }

    /**
     * Returns the SenderCompID[49] value.
     *
     * @return the SenderCompID[49] value
     */
    public String getSenderCompId() {
        return senderCompId;
    }

    /**
     * Returns the SenderCompID[49] value.
     *
     * @return the SenderCompID[49] value
     */
    public DirectBuffer getSenderCompIdAsBuffer() {
        return senderCompIdAsBuffer;
    }

    /**
     * Returns the TargetCompID[56] value.
     *
     * @return the TargetCompID[56] value
     */
    public String getTargetCompId() {
        return targetCompId;
    }

    /**
     * Returns the TargetCompID[56] value.
     *
     * @return the TargetCompID[56] value
     */
    public DirectBuffer getTargetCompIdAsBuffer() {
        return targetCompIdAsBuffer;
    }

    /**
     * Returns the Username[553] value.
     *
     * @return the Username[553] value
     */
    public String getUsername() {
        return username;
    }

    /**
     * Returns the Password[554] value.
     *
     * @return the Password[554] value
     */
    public String getPassword() {
        return password;
    }

    /**
     * Returns the HeartBtInt[108] value in seconds.
     *
     * @return the HeartBtInt[108] value in seconds
     */
    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    /**
     * Returns the Account[1] value.
     *
     * @return the Account[1] value
     */
    public String getAccount() {
        return account;
    }

    @Command(path = "state", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("version").object(fixVersion)
                .string("senderCompId").string(senderCompId)
                .string("targetCompId").string(targetCompId);

        if (username != null) {
            encoder.string("username").string(username);
        }
        if (password != null) {
            encoder.string("password").string(password);
        }
        if (account != null) {
            encoder.string("account").string(account);
        }

        encoder.string("heartbeatInterval").number(heartbeatInterval)
                .closeMap();
    }
}
