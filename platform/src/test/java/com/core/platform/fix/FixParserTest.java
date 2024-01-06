package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.log.TestLogFactory;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class FixParserTest {

    private FixLexer lexer;
    private FixMsg fixMsg;

    @BeforeEach
    void before_each() {
        var logFactory = new TestLogFactory();
        var dispatcher = mock(FixParser.Dispatcher.class);
        doAnswer(x -> {
            this.fixMsg = x.getArgument(0);
            return true;
        }).when(dispatcher).onFixMsg(any());
        lexer = new FixLexer(logFactory, new FixParser(
                logFactory, dispatcher, new FixSessionConfiguration(FixVersion.FIX42, "SENDER", "TARGET")));
    }

    @Test
    void parse_fix_message_and_get_buffers() {
        var msg = "8=FIX.4.2|9=100|35=D|49=TARGET|56=SENDER|58=HI|10=123|";

        var actual = parse(msg);

        then(actual).isEqualTo(msg.length());
        then(fixMsg.getSize()).isEqualTo(7);

        then(fixMsg.getValue(8)).isEqualTo(BufferUtils.fromAsciiString("FIX.4.2"));
        then(fixMsg.getValue(9)).isEqualTo(BufferUtils.fromAsciiString("100"));
        then(fixMsg.getValue(35)).isEqualTo(BufferUtils.fromAsciiString("D"));
        then(fixMsg.getValue(49)).isEqualTo(BufferUtils.fromAsciiString("TARGET"));
        then(fixMsg.getValue(56)).isEqualTo(BufferUtils.fromAsciiString("SENDER"));
        then(fixMsg.getValue(58)).isEqualTo(BufferUtils.fromAsciiString("HI"));
        then(fixMsg.getValue(10)).isEqualTo(BufferUtils.fromAsciiString("123"));

        then(fixMsg.getTagAt(0)).isEqualTo(8);
        then(fixMsg.getTagAt(1)).isEqualTo(9);
        then(fixMsg.getTagAt(2)).isEqualTo(35);
        then(fixMsg.getTagAt(3)).isEqualTo(49);
        then(fixMsg.getTagAt(4)).isEqualTo(56);
        then(fixMsg.getTagAt(5)).isEqualTo(58);
        then(fixMsg.getTagAt(6)).isEqualTo(10);

        then(fixMsg.getValueAt(0)).isEqualTo(BufferUtils.fromAsciiString("FIX.4.2"));
        then(fixMsg.getValueAt(1)).isEqualTo(BufferUtils.fromAsciiString("100"));
        then(fixMsg.getValueAt(2)).isEqualTo(BufferUtils.fromAsciiString("D"));
        then(fixMsg.getValueAt(3)).isEqualTo(BufferUtils.fromAsciiString("TARGET"));
        then(fixMsg.getValueAt(4)).isEqualTo(BufferUtils.fromAsciiString("SENDER"));
        then(fixMsg.getValueAt(5)).isEqualTo(BufferUtils.fromAsciiString("HI"));
        then(fixMsg.getValueAt(6)).isEqualTo(BufferUtils.fromAsciiString("123"));
    }

    @Test
    void incorrect_BeginString_returns_neg1() {
        var msg = "8=FIX.4.3|9=100|35=D|49=TARGET|56=SENDER|58=HI|10=123|";

        var actual = parse(msg);

        then(actual).isEqualTo(-1);
    }

    @Test
    void incorrect_SenderCompId_returns_neg1() {
        var msg = "8=FIX.4.2|9=100|35=D|49=TARGET1|56=SENDER|58=HI|10=123|";

        var actual = parse(msg);

        then(actual).isEqualTo(-1);
    }

    @Test
    void incorrect_TargetCompId_returns_neg1() {
        var msg = "8=FIX.4.2|9=100|35=D|49=TARGET|56=SENDER1|58=HI|10=123|";

        var actual = parse(msg);

        then(actual).isEqualTo(-1);
    }

    @Test
    void BeginString_in_wrong_order_returns_neg1() {
        var msg = "40=1|8=FIX.4.2|9=100|35=D|49=TARGET|56=SENDER|58=HI|10=123|";

        var actual = parse(msg);

        then(actual).isEqualTo(-1);
    }

    @Test
    void BodyLength_in_wrong_order_returns_neg1() {
        var msg = "8=FIX.4.2|40=1|9=100|35=D|49=TARGET|56=SENDER|58=HI|10=123|";

        var actual = parse(msg);

        then(actual).isEqualTo(-1);
    }

    @Test
    void MsgType_in_wrong_order_returns_neg1() {
        var msg = "8=FIX.4.2|9=100|40=1|35=D|49=TARGET|56=SENDER|58=HI|10=123|";

        var actual = parse(msg);

        then(actual).isEqualTo(-1);
    }

    @Test
    void contains_tag() {
        var msg = "8=FIX.4.2|9=100|35=D|49=TARGET|56=SENDER|58=HI|10=123|";

        parse(msg);

        then(fixMsg.containsTag(9)).isTrue();
        then(fixMsg.containsTag(58)).isTrue();
        then(fixMsg.containsTag(100)).isFalse();
    }

    @Test
    void parse_integer() {
        var msg = "8=FIX.4.2|9=100|35=D|49=TARGET|56=SENDER|58=HI|10=123|";

        parse(msg);

        then(fixMsg.getValueAsInteger(9)).isEqualTo(100);
    }

    @Test
    void parse_bad_integer_returns_neg1() {
        var msg = "8=FIX.4.2|9=100a|35=D|49=TARGET|56=SENDER|58=HI|10=123|";

        parse(msg);

        then(fixMsg.getValueAsInteger(9)).isEqualTo(-1);
    }

    @Test
    void parse_integer_with_default_value() {
        var msg = "8=FIX.4.2|9=100a|35=D|49=TARGET|56=SENDER|58=HI|10=123|";

        parse(msg);

        then(fixMsg.getValueAsInteger(9, -2)).isEqualTo(-2);
    }

    @Test
    void parse_floatingPoint() {
        var msg = "8=FIX.4.2|9=100.123|35=D|49=TARGET|56=SENDER|58=HI|10=123|";

        parse(msg);

        then(fixMsg.getValueAsDouble(9)).isEqualTo(100.123);
    }

    @Test
    void parse_floatingPoint_unknown_tag_returns_neg1() {
        var msg = "8=FIX.4.2|9=100.123|35=D|49=TARGET|56=SENDER|58=HI|10=123|";

        parse(msg);

        then(fixMsg.getValueAsDouble(513)).isEqualTo(-1);
    }

    @Test
    void parse_floatingPoint_with_specified_defaultValue() {
        var msg = "8=FIX.4.2|9=100.123|35=D|49=TARGET|56=SENDER|58=HI|10=123|";

        parse(msg);

        then(fixMsg.getValueAsDouble(513, -2)).isEqualTo(-2);
    }

    @Test
    void getEnum_returns_an_enum_for_the_value() {
        var msg = "8=FIX.4.2|9=100|35=D|49=TARGET|56=SENDER|54=1|10=123|";

        parse(msg);

        then(fixMsg.getValueAsEnum(54, Side::apply)).isEqualTo(Side.BUY);
    }

    @Test
    void getEnum_returns_null_for_an_unknown_tag() {
        var msg = "8=FIX.4.2|9=100|35=D|49=TARGET|56=SENDER|10=123|";

        parse(msg);

        then(fixMsg.getValueAsEnum(54, Side::apply)).isNull();
    }

    @Test
    void getRepeatingGroup() {
        var msg = "8=FIX.4.2|9=100|35=D|49=TARGET|56=SENDER|146=3|"
                + "55=BTC/USD|207=COINBASE|460=1|15=USD|"
                + "55=BTC/USD|207=BINANCE|460=4|"
                + "55=ETH/USD|207=COINBASE|406=6|"
                + "10=123|";

        parse(msg);

        var groups = fixMsg.getRepeatingGroups(146, 55);
        then(groups.size()).isEqualTo(3);

        var group = groups.get(0);
        then(group.getSize()).isEqualTo(4);
        then(group.getTagAt(0)).isEqualTo(55);
        then(group.getTagAt(1)).isEqualTo(207);
        then(group.getTagAt(2)).isEqualTo(460);
        then(group.getTagAt(3)).isEqualTo(15);
        then(group.getValueAt(0)).isEqualTo(BufferUtils.fromAsciiString("BTC/USD"));
        then(group.getValueAt(1)).isEqualTo(BufferUtils.fromAsciiString("COINBASE"));
        then(group.getValueAsIntegerAt(2)).isEqualTo(1L);
        then(group.getValueAt(3)).isEqualTo(BufferUtils.fromAsciiString("USD"));
        then(group.getValue(55)).isEqualTo(BufferUtils.fromAsciiString("BTC/USD"));
        then(group.getValue(207)).isEqualTo(BufferUtils.fromAsciiString("COINBASE"));
        then(group.getValueAsInteger(460)).isEqualTo(1L);
        then(group.getValue(15)).isEqualTo(BufferUtils.fromAsciiString("USD"));

        group = groups.get(1);
        then(group.getSize()).isEqualTo(3);
        then(group.getTagAt(0)).isEqualTo(55);
        then(group.getTagAt(1)).isEqualTo(207);
        then(group.getTagAt(2)).isEqualTo(460);
        then(group.getValueAt(0)).isEqualTo(BufferUtils.fromAsciiString("BTC/USD"));
        then(group.getValueAt(1)).isEqualTo(BufferUtils.fromAsciiString("BINANCE"));
        then(group.getValueAsIntegerAt(2)).isEqualTo(4L);
        then(group.getValue(55)).isEqualTo(BufferUtils.fromAsciiString("BTC/USD"));
        then(group.getValue(207)).isEqualTo(BufferUtils.fromAsciiString("BINANCE"));
        then(group.getValueAsInteger(460)).isEqualTo(4L);

        group = groups.get(2);
        then(group.getSize()).isEqualTo(4);
        then(group.getTagAt(0)).isEqualTo(55);
        then(group.getTagAt(1)).isEqualTo(207);
        then(group.getTagAt(2)).isEqualTo(406);
        then(group.getTagAt(3)).isEqualTo(10);
        then(group.getValueAt(0)).isEqualTo(BufferUtils.fromAsciiString("ETH/USD"));
        then(group.getValueAt(1)).isEqualTo(BufferUtils.fromAsciiString("COINBASE"));
        then(group.getValueAsIntegerAt(2)).isEqualTo(6L);
        then(group.getValueAsIntegerAt(3)).isEqualTo(123L);
        then(group.getValue(55)).isEqualTo(BufferUtils.fromAsciiString("ETH/USD"));
        then(group.getValue(207)).isEqualTo(BufferUtils.fromAsciiString("COINBASE"));
        then(group.getValueAsInteger(406)).isEqualTo(6L);
        then(group.getValueAsInteger(10)).isEqualTo(123L);
    }

    @Test
    void getRepeatingGroup_with_unknown_numGroups_returns_zero_list() {
        var msg = "8=FIX.4.2|9=100|35=D|49=TARGET|56=SENDER|146=3|"
                + "55=BTC/USD|207=COINBASE|460=1|15=USD|"
                + "55=BTC/USD|207=BINANCE|460=4|"
                + "55=ETH/USD|207=COINBASE|406=6|"
                + "10=123|";

        parse(msg);

        var groups = fixMsg.getRepeatingGroups(147, 55);
        then(groups.size()).isEqualTo(0);
    }

    @Test
    void getRepeatingGroup_with_incorrect_firstTagInGroup_returns_zero_list() {
        var msg = "8=FIX.4.2|9=100|35=D|49=TARGET|56=SENDER|146=3|"
                + "55=BTC/USD|207=COINBASE|460=1|15=USD|"
                + "55=BTC/USD|207=BINANCE|460=4|"
                + "55=ETH/USD|207=COINBASE|406=6|"
                + "10=123|";

        parse(msg);

        var groups = fixMsg.getRepeatingGroups(146, 207);
        then(groups.size()).isEqualTo(0);
    }

    @Test
    void getRepeatingGroup_with_more_groups_than_numGroupsTag_returns_smaller_list() {
        var msg = "8=FIX.4.2|9=100|35=D|49=TARGET|56=SENDER|146=2|"
                + "55=BTC/USD|207=COINBASE|460=1|15=USD|"
                + "55=BTC/USD|207=BINANCE|460=4|"
                + "55=ETH/USD|207=COINBASE|406=6|"
                + "10=123|";

        parse(msg);

        var groups = fixMsg.getRepeatingGroups(146, 55);
        then(groups.size()).isEqualTo(2);
    }

    @Test
    void getRepeatingGroup_with_less_groups_than_numGroupsTag_returns_smaller_list() {
        var msg = "8=FIX.4.2|9=100|35=D|49=TARGET|56=SENDER|146=4|"
                + "55=BTC/USD|207=COINBASE|460=1|15=USD|"
                + "55=BTC/USD|207=BINANCE|460=4|"
                + "55=ETH/USD|207=COINBASE|406=6|"
                + "10=123|";

        parse(msg);

        var groups = fixMsg.getRepeatingGroups(146, 55);
        then(groups.size()).isEqualTo(3);
    }

    private int parse(String fix) {
        var string = fix.replace('|', (char) FixUtils.SOH);
        var buffer = BufferUtils.fromAsciiString(string);
        return lexer.lex(buffer, 0, buffer.capacity());
    }

    private enum Side {

        BUY,
        SELL;

        public static Side apply(DirectBuffer buffer) {
            if (buffer == null || buffer.capacity() != 1) {
                return null;
            }

            var theByte = buffer.getByte(0);
            if (theByte == '1') {
                return BUY;
            } else if (theByte == '2') {
                return SELL;
            } else {
                return null;
            }
        }
    }
}
