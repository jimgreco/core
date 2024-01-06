package com.core.platform.shell;

import com.core.infrastructure.encoding.MutableObjectEncoder;

interface ShellContextHandler {

    void onClosed();

    MutableObjectEncoder getObjectEncoder();

    AsyncCommandContext borrowAsyncCommandContext();
}
