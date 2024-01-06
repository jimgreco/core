package com.core.platform.shell;

import com.core.infrastructure.encoding.EncoderUtils;
import com.core.infrastructure.encoding.MutableObjectEncoder;

/**
 * The {@code TextShellContextHandler} is used for the initial load of the shell with command files.
 */
public class TextShellContextHandler implements ShellContextHandler {

    private final MutableObjectEncoder encoder;

    /**
     * Constructs an empty {@code TextShellContextHandler}.
     */
    public TextShellContextHandler() {
        encoder = EncoderUtils.createTextEncoder();
    }

    @Override
    public void onClosed() {
    }

    @Override
    public MutableObjectEncoder getObjectEncoder() {
        return encoder;
    }

    @Override
    public AsyncCommandContext borrowAsyncCommandContext() {
        throw new IllegalStateException("TextShellContextHandler does not support streaming commands");
    }
}
