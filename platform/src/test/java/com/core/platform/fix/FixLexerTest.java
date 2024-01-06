package com.core.platform.fix;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.log.TestLogFactory;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class FixLexerTest {

    private FixLexer.Parser parser;
    private FixLexer lexer;
    private List<TagValue> tagValues;

    @BeforeEach
    void before_each() {
        parser = mock(FixLexer.Parser.class);
        tagValues = new ArrayList<>();
        doAnswer(x -> {
            var tag = (int) x.getArgument(0);
            var buffer = (DirectBuffer) x.getArgument(1);
            var offset = (int) x.getArgument(2);
            var length = (int) x.getArgument(3);
            tagValues.add(new TagValue(tag, BufferUtils.toAsciiString(buffer, offset, length)));
            return true;
        }).when(parser).onField(anyInt(), any(), anyInt(), anyInt());
        doAnswer(x -> true).when(parser).end(anyInt());
        lexer = new FixLexer(new TestLogFactory(), parser);
    }

    @Test
    void invoke_start_on_parse() {
        parse("");

        verify(parser).start(any(), anyInt());
    }

    @Test
    void do_not_invoke_end_if_not_tag_10() {
        parse("8=FIX.4.2|9=100|35=A|49=FOO|56=BAR|58=HI|");

        verify(parser, never()).end(anyInt());
    }

    @Test
    void invoke_end_on_tag_10() {
        var msg = "8=FIX.4.2|9=100|35=A|49=FOO|56=BAR|58=HI|10=123|";

        parse(msg);

        verify(parser).start(any(), eq(0));
        verify(parser).end(msg.length());
    }

    @Test
    void parse_complete_fix_message_returns_message_length() {
        var msg = "8=FIX.4.2|9=100|35=A|49=FOO|56=BAR|58=HI|10=123|";

        var actual = parse(msg);

        then(actual).isEqualTo(msg.length());
        then(tagValues.size()).isEqualTo(7);
        then(tagValues.get(0).tag).isEqualTo(8);
        then(tagValues.get(1).tag).isEqualTo(9);
        then(tagValues.get(2).tag).isEqualTo(35);
        then(tagValues.get(3).tag).isEqualTo(49);
        then(tagValues.get(4).tag).isEqualTo(56);
        then(tagValues.get(5).tag).isEqualTo(58);
        then(tagValues.get(6).tag).isEqualTo(10);

        then(tagValues.get(0).value).isEqualTo("FIX.4.2");
        then(tagValues.get(1).value).isEqualTo("100");
        then(tagValues.get(2).value).isEqualTo("A");
        then(tagValues.get(3).value).isEqualTo("FOO");
        then(tagValues.get(4).value).isEqualTo("BAR");
        then(tagValues.get(5).value).isEqualTo("HI");
        then(tagValues.get(6).value).isEqualTo("123");
    }

    @Test
    void parse_complete_fix_message_stops_at_first_tag_10() {
        var msg1 = "8=FIX.4.2|9=100|35=A|49=FOO|56=BAR|58=HI|10=123|";
        var msg = msg1 + "8=FIX.4.4|9=89|49=SO|56=WHAT|10=456|";

        var actual = parse(msg);

        then(actual).isEqualTo(msg1.length());
        then(tagValues.size()).isEqualTo(7);
        then(tagValues.get(0).tag).isEqualTo(8);
        then(tagValues.get(1).tag).isEqualTo(9);
        then(tagValues.get(2).tag).isEqualTo(35);
        then(tagValues.get(3).tag).isEqualTo(49);
        then(tagValues.get(4).tag).isEqualTo(56);
        then(tagValues.get(5).tag).isEqualTo(58);
        then(tagValues.get(6).tag).isEqualTo(10);

        then(tagValues.get(0).value).isEqualTo("FIX.4.2");
        then(tagValues.get(1).value).isEqualTo("100");
        then(tagValues.get(2).value).isEqualTo("A");
        then(tagValues.get(3).value).isEqualTo("FOO");
        then(tagValues.get(4).value).isEqualTo("BAR");
        then(tagValues.get(5).value).isEqualTo("HI");
        then(tagValues.get(6).value).isEqualTo("123");
    }

    @Test
    void parse_complete_fix_message_with_offset() {
        var msg1 = "8=FIX.4.2|9=100|35=A|49=FOO|56=BAR|58=HI|10=123|";
        var msg = msg1 + "8=FIX.4.4|9=89|35=1|49=SO|56=WHAT|10=456|";

        var actual = parse(msg, msg1.length());

        then(actual).isEqualTo(msg.length() - msg1.length());
        then(tagValues.size()).isEqualTo(6);
        then(tagValues.get(0).tag).isEqualTo(8);
        then(tagValues.get(1).tag).isEqualTo(9);
        then(tagValues.get(2).tag).isEqualTo(35);
        then(tagValues.get(3).tag).isEqualTo(49);
        then(tagValues.get(4).tag).isEqualTo(56);
        then(tagValues.get(5).tag).isEqualTo(10);

        then(tagValues.get(0).value).isEqualTo("FIX.4.4");
        then(tagValues.get(1).value).isEqualTo("89");
        then(tagValues.get(2).value).isEqualTo("1");
        then(tagValues.get(3).value).isEqualTo("SO");
        then(tagValues.get(4).value).isEqualTo("WHAT");
        then(tagValues.get(5).value).isEqualTo("456");
    }

    @Test
    void parse_incomplete_fix_message_returns_zero() {
        var msg = "8=FIX.4.2|9=100|49=FOO|56=BAR|58=HI|10=123";

        var actual = parse(msg);

        then(actual).isEqualTo(0);
    }

    @Test
    void parser_returns_false_causes_return_neg1() {
        var msg = "8=FIX.4.2|9=100|49=FOO|56=BAR|58=HI|10=123|";
        doAnswer(x -> true).doAnswer(x -> true).doAnswer(x -> false)
                .when(parser).onField(anyInt(), any(), anyInt(), anyInt());

        var actual = parse(msg);

        then(actual).isEqualTo(-1);
    }

    @Test
    void empty_tag_returns_returns_neg1() {
        var msg = "8=FIX.4.2|=100|49=FOO|56=BAR|58=HI|10=123|";

        var actual = parse(msg);

        then(actual).isEqualTo(-1);
    }

    @Test
    void illegal_tag_character_returns_neg1() {
        var msg = "8=FIX.4.2|9a1=100|49=FOO|56=BAR|58=HI|10=123|";

        var actual = parse(msg);

        then(actual).isEqualTo(-1);
    }

    @Test
    void empty_value_is_allowed() {
        var msg = "8=FIX.4.2|9=100|35=A|49=|56=BAR|58=HI|10=123|";

        var actual = parse(msg);

        then(actual).isEqualTo(msg.length());
        then(tagValues.size()).isEqualTo(7);
        then(tagValues.get(0).tag).isEqualTo(8);
        then(tagValues.get(1).tag).isEqualTo(9);
        then(tagValues.get(2).tag).isEqualTo(35);
        then(tagValues.get(3).tag).isEqualTo(49);
        then(tagValues.get(4).tag).isEqualTo(56);
        then(tagValues.get(5).tag).isEqualTo(58);
        then(tagValues.get(6).tag).isEqualTo(10);

        then(tagValues.get(0).value).isEqualTo("FIX.4.2");
        then(tagValues.get(1).value).isEqualTo("100");
        then(tagValues.get(2).value).isEqualTo("A");
        then(tagValues.get(3).value).isEqualTo("");
        then(tagValues.get(4).value).isEqualTo("BAR");
        then(tagValues.get(5).value).isEqualTo("HI");
        then(tagValues.get(6).value).isEqualTo("123");
    }

    private int parse(String fix) {
        return parse(fix, 0);
    }

    private int parse(String fix, int offset) {
        var string = fix.substring(offset).replace('|', (char) FixUtils.SOH);
        var buffer = BufferUtils.fromAsciiString(string);
        return lexer.lex(buffer, 0, buffer.capacity());
    }
}
