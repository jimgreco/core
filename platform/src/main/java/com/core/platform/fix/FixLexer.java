package com.core.platform.fix;

import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.platform.fix.schema.Fix42;
import org.agrona.DirectBuffer;

class FixLexer {

    private final Log log;
    private final Parser parser;

    FixLexer(LogFactory logFactory, Parser parser) {
        this.log = logFactory.create(FixLexer.class);
        this.parser = parser;
    }

    int lex(DirectBuffer buffer, int offset, int length) {
        var tag = 0;
        var valueStartByte = -1;
        parser.start(buffer, offset);

        for (var i = offset; i < offset + length; i++) {
            var character = buffer.getByte(i);
            if (valueStartByte == -1) {
                // in tag
                if (character >= '0' && character <= '9') {
                    tag *= 10;
                    tag += character - '0';
                } else if (character == '=') {
                    if (tag == 0) {
                        log.warn().append("empty tag, disconnecting: index=").append(i)
                                .append(", buffer=").append(buffer, offset, length)
                                .commit();
                        return -1;
                    }
                    valueStartByte = i + 1;
                } else {
                    log.warn().append("illegal character in tag, disconnecting: character=").append(character)
                            .append(", index=").append(i)
                            .append(", buffer=").append(buffer, offset, length)
                            .commit();
                    return -1;
                }
            } else if (character == FixUtils.SOH) {
                // end of value
                if (!parser.onField(tag, buffer, valueStartByte, i - valueStartByte)) {
                    return -1;
                }

                if (tag == Fix42.CHECK_SUM) {
                    // done parsing FIX message
                    var endByte = i + 1;
                    return parser.end(endByte) ? endByte - offset : -1;
                }

                tag = 0;
                valueStartByte = -1;
            }
        }

        return 0;
    }

    interface Parser {

        void start(DirectBuffer buffer, int offset);

        boolean onField(int tag, DirectBuffer value, int offset, int length);

        boolean end(int offset);
    }
}
