package com.core.platform.shell;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.buffer.BufferNumberUtils;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

class StatementParser {

    static final int MAX_TOKENS = 25;
    static final int INITIAL_TOKEN_SIZE = 50;

    private final CommandParser commandParser;
    private final MutableDirectBuffer[] mutableTokens;
    private final DirectBuffer[] tokens;
    private final MutableDirectBuffer variable;
    private final DirectBuffer variableWrapper;
    private int numTokens;
    private int tokenPosition;
    private boolean inQuotes;
    private boolean backslash;
    private boolean inComment;
    private boolean inVariable;
    private int variablePosition;

    StatementParser(CommandParser commandParser) {
        this.commandParser = commandParser;
        mutableTokens = new MutableDirectBuffer[MAX_TOKENS];
        tokens = new DirectBuffer[MAX_TOKENS];
        for (var i = 0; i < mutableTokens.length; i++) {
            mutableTokens[i] = BufferUtils.allocateExpandable(INITIAL_TOKEN_SIZE);
            tokens[i] = BufferUtils.emptyBuffer();
        }
        variable = BufferUtils.allocate(INITIAL_TOKEN_SIZE);
        variableWrapper = BufferUtils.emptyBuffer();
    }

    /**
     * Clears all values in the statement parser.
     */
    void clear() {
        numTokens = 0;
        tokenPosition = 0;
        variablePosition = 0;
        inQuotes = false;
        backslash = false;
        inComment = false;
        inVariable = false;
    }

    void flush(Shell.ShellContextImpl context) throws CommandException {
        if (inQuotes) {
            throw new CommandException("cannot terminate line inside quotes", 0);
        }
        if (backslash) {
            throw new CommandException("cannot terminate file with backslash", 0);
        }
        endToken(context, 0);
        if (numTokens > 0) {
            endStatement(context);
        }
    }

    /**
     * Parses a single statement from the specified {@code buffer}.
     *
     * <p>A statement consists of one or more words and terminated by a newline.
     * Words are separated by whitespace.
     * Double quotes allow for whitespaces inside of words.
     * Backslashes allow for special characters inside of words: double quotes, backslashes, newlines, tabs,
     * dollar signs, and at signs.
     * A statement will not terminate at the newline character if a backslash immediately precedes it.
     *
     * <p>This method can be invoked with additional data in the buffer and will continue from where parsing stopped.
     *
     * @param context the shell context
     * @param buffer the buffer to parse
     * @param offset the first byte of the buffer to parse
     * @param length the number of bytes in the buffer to parse
     * @return the number of bytes parsed
     * @throws CommandException if the statement could not be parsed
     */
    int parse(ShellContext context, DirectBuffer buffer, int offset, int length) throws CommandException {
        var position = offset;
        while (position < offset + length) {
            var theByte = buffer.getByte(position);
            position++;

            if (theByte == '\n' || !inQuotes && !inComment && theByte == ';') {
                // newline is the end of the statement, unless immediately preceded by a backlash
                if (inQuotes) {
                    throw new CommandException("cannot terminate line inside quotes", position);
                }
                var wasInComment = inComment;
                if (inComment) {
                    inComment = false;
                }
                endToken(context, position);
                if (!wasInComment && theByte == '\n' && backslash) {
                    backslash = false;
                } else if (numTokens > 0) {
                    endStatement(context);
                    return position - offset;
                }
            } else if (!inComment) {
                if (backslash) {
                    // previous character was a backslash, this character is a special character
                    backslash = false;
                    if (theByte == '"' || theByte == '\\' || theByte == '#' || theByte == '$') {
                        addByteToToken(context, (char) theByte, position);
                    } else if (theByte == 'n') {
                        addByteToToken(context, '\n', position);
                    } else if (theByte == 't') {
                        addByteToToken(context, '\t', position);
                    } else {
                        throw new CommandException("illegal character after backslash: " + (char) theByte, position);
                    }
                } else if (theByte == '\\') {
                    // next character is a special character
                    backslash = true;
                } else if (theByte == '"') {
                    if (inQuotes) {
                        endToken(context, position);
                        inQuotes = false;
                    } else {
                        inQuotes = true;
                    }
                } else if (theByte == '#') {
                    inComment = true;
                } else if (theByte == '$') {
                    if (inVariable) {
                        endVariable(context, position);
                    }
                    inVariable = true;
                } else if (Character.isWhitespace(theByte)) {
                    if (inQuotes) {
                        // whitespace can be inside quotes
                        addByteToToken(context, (char) theByte, position);
                    } else {
                        // white space at the beginning of a statement is ignored
                        endToken(context, position);
                    }
                } else {
                    addByteToToken(context, (char) theByte, position);
                }
            }
        }
        return position - offset;
    }

    private void addByteToToken(ShellContext context, char theByte, int position) throws CommandException {
        if (inComment) {
            return;
        }

        if (inVariable) {
            if (theByte >= '0' && theByte <= '9'
                    || theByte >= 'A' && theByte <= 'Z'
                    || theByte >= 'a' && theByte <= 'z'
                    || theByte == '_') {
                variable.putByte(variablePosition, (byte) theByte);
                variablePosition++;
            } else {
                endVariable(context, position);

                if (!Character.isWhitespace(theByte)) {
                    addByteToToken(context, theByte, position);
                }
            }
        } else {
            if (numTokens == tokens.length) {
                throw new CommandException("statement has a max of 100 words", position);
            }
            mutableTokens[numTokens].putByte(tokenPosition, (byte) theByte);
            tokenPosition++;
        }
    }

    private void endVariable(ShellContext context, int bufferPosition) throws CommandException {
        inVariable = false;
        variableWrapper.wrap(variable, 0, variablePosition);
        variablePosition = 0;
        var variableValue = context.getVariables().get(variableWrapper);
        if (variableValue == null) {
            var args = context.getArguments();
            if (args != null) {
                var argNumber = (int) BufferNumberUtils.parseAsLong(variableWrapper);
                variableValue = argNumber >= 0 && argNumber < args.size() ? args.get(argNumber) : null;
            }
            if (variableValue == null) {
                throw new CommandException(
                        "unset variable: " + BufferUtils.toAsciiString(variableWrapper), bufferPosition);
            }
        }

        if (numTokens == tokens.length) {
            throw new CommandException("statement has a max of 100 words", bufferPosition);
        }
        mutableTokens[numTokens].putBytes(
                tokenPosition, variableValue, 0, variableValue.capacity());
        tokenPosition += variableValue.capacity();
    }

    private void endToken(ShellContext context, int bufferPosition) throws CommandException {
        if (numTokens == tokens.length) {
            throw new CommandException("statement has a max of 25 words", bufferPosition);
        }
        if (inVariable) {
            endVariable(context, bufferPosition);
        }
        if (inQuotes || tokenPosition > 0) {
            tokens[numTokens].wrap(mutableTokens[numTokens], 0, tokenPosition);
            numTokens++;
            tokenPosition = 0;
        }
    }

    private void endStatement(ShellContext context) throws CommandException {
        commandParser.process(context, tokens[0], tokens, 1, numTokens - 1);
        clear();
    }

    interface CommandParser {

        void process(ShellContext context, DirectBuffer command, DirectBuffer[] params, int index, int length)
                throws CommandException;
    }
}
