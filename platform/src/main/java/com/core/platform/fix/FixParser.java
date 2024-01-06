package com.core.platform.fix;

import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.platform.fix.schema.Fix42;
import org.agrona.DirectBuffer;

class FixParser implements FixLexer.Parser {

    private final Log log;
    private final Dispatcher dispatcher;

    private final BufferFixMsg fixMsg;
    private final FixSessionConfiguration sessionConfiguration;

    FixParser(LogFactory logFactory, Dispatcher dispatcher, FixSessionConfiguration sessionConfiguration) {
        log = logFactory.create(FixParser.class);
        this.dispatcher = dispatcher;
        fixMsg = new BufferFixMsg();
        this.sessionConfiguration = sessionConfiguration;
    }

    @Override
    public void start(DirectBuffer buffer, int offset) {
        fixMsg.start(buffer, offset);
    }

    @Override
    public boolean onField(int tag, DirectBuffer valueBuffer, int offset, int length) {
        var tagIndex = fixMsg.getSize();

        var value = fixMsg.addField(tag, valueBuffer, offset, length);
        if (value == null) {
            log.error().append("too many tags in FIX message: maxTags=").append(tagIndex).commit();
            return false;
        }

        if (tag == Fix42.BEGIN_STRING
                && (tagIndex != 0 || !sessionConfiguration.getFixVersion().getBeginString().equals(value))) {
            log.warn().append("unexpected BeginString[8] value/location, disconnecting: tagIndex=").append(tagIndex)
                    .append(", expected=").append(sessionConfiguration.getFixVersion().getBeginString())
                    .append(", received=").append(value)
                    .commit();
            return false;
        } else if (tag == Fix42.BODY_LENGTH && tagIndex != 1) {
            log.warn().append("unexpected BodyLength[9] location, disconnecting: tagIndex=").append(tagIndex).commit();
            return false;
        } else if (tag == Fix42.MSG_TYPE && tagIndex != 2) {
            log.warn().append("unexpected MsgType[35] location, disconnecting: tagIndex=").append(tagIndex).commit();
            return false;
        } else if (tag == Fix42.SENDER_COMP_ID
                && !sessionConfiguration.getTargetCompIdAsBuffer().equals(value)) {
            log.warn().append("unexpected SenderCompID[49], disconnecting: tagIndex=").append(tagIndex)
                    .append(", expected=").append(sessionConfiguration.getTargetCompId())
                    .append(", received=").append(value)
                    .commit();
            return false;
        } else if (tag == Fix42.TARGET_COMP_ID
                && !sessionConfiguration.getSenderCompIdAsBuffer().equals(value)) {
            log.warn().append("unexpected TargetCompID[56], disconnecting: tagIndex=").append(tagIndex)
                    .append(", expected=").append(sessionConfiguration.getSenderCompId())
                    .append(", received=").append(value)
                    .commit();
            return false;
        }

        return true;
    }

    @Override
    public boolean end(int offset) {
        fixMsg.end(offset);
        return dispatcher.onFixMsg(fixMsg);
    }

    interface Dispatcher {

        boolean onFixMsg(FixMsg fixMsg);
    }
}
