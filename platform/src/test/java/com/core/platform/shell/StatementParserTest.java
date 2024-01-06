package com.core.platform.shell;

import com.core.infrastructure.buffer.BufferUtils;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatementParserTest {

    private List<DirectBuffer[]> statements;
    private Map<DirectBuffer, DirectBuffer> variables;
    private List<DirectBuffer> args;
    private StatementParser statementParser;

    @BeforeEach
    void before_each() throws CommandException {
        statements = new ArrayList<>();
        var commandParser = mock(StatementParser.CommandParser.class);
        variables = new HashMap<>();
        args = new ArrayList<>();
        statementParser = new StatementParser(commandParser);
        doAnswer(x -> {
            var statement = new DirectBuffer[(int) x.getArgument(4) + 1];
            statement[0] = x.getArgument(1);
            var params = (DirectBuffer[]) x.getArgument(2);
            var offset = (int) x.getArgument(3);
            for (var i = 1; i < statement.length; i++) {
                statement[i] = BufferUtils.copy(params[offset + i - 1]);
            }
            statements.add(statement);
            return null;
        }).when(commandParser).process(any(), any(), any(), anyInt(), anyInt());
    }

    @Test
    void parse_simple_words() throws CommandException {
        var input = BufferUtils.fromAsciiString("foo bar me\n");

        var parsed = parse(input);

        then(parsed).isEqualTo(input.capacity());
        then(statements.size()).isEqualTo(1);
        var expected = statements.get(0);
        then(expected.length).isEqualTo(3);
        then(expected[0]).isEqualTo(BufferUtils.fromAsciiString("foo"));
        then(expected[1]).isEqualTo(BufferUtils.fromAsciiString("bar"));
        then(expected[2]).isEqualTo(BufferUtils.fromAsciiString("me"));
    }

    @Test
    void parse_words_with_whitespace() throws CommandException {
        var input = BufferUtils.fromAsciiString(" \t foo   bar   me\t   \n");

        var parsed = parse(input);

        then(parsed).isEqualTo(input.capacity());
        var statement = statements.get(0);
        then(statement.length).isEqualTo(3);
        then(statement[0]).isEqualTo(BufferUtils.fromAsciiString("foo"));
        then(statement[1]).isEqualTo(BufferUtils.fromAsciiString("bar"));
        then(statement[2]).isEqualTo(BufferUtils.fromAsciiString("me"));
    }

    @Test
    void parse_words_with_special_characters() throws CommandException {
        var input = BufferUtils.fromAsciiString(" \t foo   \"b\\\\a\\\"r\"   me\t   \n");

        var parsed = parse(input);

        then(parsed).isEqualTo(input.capacity());
        var statement = statements.get(0);
        then(statement.length).isEqualTo(3);
        then(statement[0]).isEqualTo(BufferUtils.fromAsciiString("foo"));
        then(statement[1]).isEqualTo(BufferUtils.fromAsciiString("b\\a\"r"));
        then(statement[2]).isEqualTo(BufferUtils.fromAsciiString("me"));
    }

    @Test
    void allow_spaces_in_quotes() throws CommandException {
        var input = BufferUtils.fromAsciiString("\"foo   bar\"   me\n");

        var parsed = parse(input);

        then(parsed).isEqualTo(input.capacity());
        var statement = statements.get(0);
        then(statement.length).isEqualTo(2);
        then(statement[0]).isEqualTo(BufferUtils.fromAsciiString("foo   bar"));
        then(statement[1]).isEqualTo(BufferUtils.fromAsciiString("me"));
    }

    @Test
    void endline_in_string_throws_error() {
        thenThrownBy(() -> parse(BufferUtils.fromAsciiString("\"foo   bar\n")))
                .isInstanceOf(CommandException.class);
    }

    @Test
    void continue_statement_on_next_line() throws CommandException {
        var input = BufferUtils.fromAsciiString("foo  \\\n bar   me\n");

        var parsed = parse(input);

        then(parsed).isEqualTo(input.capacity());
        var statement = statements.get(0);
        then(statement.length).isEqualTo(3);
        then(statement[0]).isEqualTo(BufferUtils.fromAsciiString("foo"));
        then(statement[1]).isEqualTo(BufferUtils.fromAsciiString("bar"));
        then(statement[2]).isEqualTo(BufferUtils.fromAsciiString("me"));
    }

    @Test
    void ignore_commented_line() throws CommandException {
        var input = BufferUtils.fromAsciiString("# foo bar me\n");

        var parsed = parse(input);

        then(parsed).isEqualTo(input.capacity());
        then(statements.size()).isEqualTo(0);
    }

    @Test
    void ignore_rest_of_commented_line() throws CommandException {
        var input = BufferUtils.fromAsciiString("foo bar  #me\n");

        var parsed = parse(input);

        then(parsed).isEqualTo(input.capacity());
        var statement = statements.get(0);
        then(statement.length).isEqualTo(2);
        then(statement[0]).isEqualTo(BufferUtils.fromAsciiString("foo"));
        then(statement[1]).isEqualTo(BufferUtils.fromAsciiString("bar"));
    }

    @Test
    void ignore_rest_of_commented_line_at_end_of_param() throws CommandException {
        var input = BufferUtils.fromAsciiString("foo bar#me\n");

        var parsed = parse(input);

        then(parsed).isEqualTo(input.capacity());
        var statement = statements.get(0);
        then(statement.length).isEqualTo(2);
        then(statement[0]).isEqualTo(BufferUtils.fromAsciiString("foo"));
        then(statement[1]).isEqualTo(BufferUtils.fromAsciiString("bar"));
    }

    @Test
    void empty_string_as_token() throws CommandException {
        var input = BufferUtils.fromAsciiString("foo \"\" bar\n");

        var parsed = parse(input);

        then(parsed).isEqualTo(input.capacity());
        var statement = statements.get(0);
        then(statement.length).isEqualTo(3);
        then(statement[0]).isEqualTo(BufferUtils.fromAsciiString("foo"));
        then(statement[1]).isEqualTo(BufferUtils.emptyBuffer());
        then(statement[2]).isEqualTo(BufferUtils.fromAsciiString("bar"));
    }

    @Test
    void stop_parsing_statement_at_newline() throws CommandException {
        var input = BufferUtils.fromAsciiString("foo bar\nme\n");

        var parsed = parse(input);

        then(parsed).isEqualTo("foo bar\n".length());
        var statement = statements.get(0);
        then(statement.length).isEqualTo(2);
        then(statement[0]).isEqualTo(BufferUtils.fromAsciiString("foo"));
        then(statement[1]).isEqualTo(BufferUtils.fromAsciiString("bar"));
    }

    @Test
    void throw_exception_for_double_dollar_sign() {
        thenThrownBy(() -> parse(BufferUtils.fromAsciiString(" foo foo$$bar  bar\n")))
                .isInstanceOf(CommandException.class);
    }

    @Test
    void throw_exception_for_unknown_variable() {
        thenThrownBy(() -> parse(BufferUtils.fromAsciiString(" foo $bar  bar\n")))
                .isInstanceOf(CommandException.class);
    }

    @Test
    void replace_variable() throws CommandException {
        variables.put(BufferUtils.fromAsciiString("bar"), BufferUtils.fromAsciiString("soo"));
        var input = BufferUtils.fromAsciiString("foo $bar\n");

        parse(input);

        var statement = statements.get(0);
        then(statement.length).isEqualTo(2);
        then(statement[0]).isEqualTo(BufferUtils.fromAsciiString("foo"));
        then(statement[1]).isEqualTo(BufferUtils.fromAsciiString("soo"));
    }

    @Test
    void replace_multiple_variables() throws CommandException {
        variables.put(BufferUtils.fromAsciiString("bar"), BufferUtils.fromAsciiString("soo"));
        variables.put(BufferUtils.fromAsciiString("doo"), BufferUtils.fromAsciiString("ya"));
        var input = BufferUtils.fromAsciiString("foo $bar$doo\n");

        parse(input);

        var statement = statements.get(0);
        then(statement.length).isEqualTo(2);
        then(statement[0]).isEqualTo(BufferUtils.fromAsciiString("foo"));
        then(statement[1]).isEqualTo(BufferUtils.fromAsciiString("sooya"));
    }

    @Test
    void replace_multiple_variables_with_space() throws CommandException {
        variables.put(BufferUtils.fromAsciiString("bar"), BufferUtils.fromAsciiString("soo"));
        variables.put(BufferUtils.fromAsciiString("doo"), BufferUtils.fromAsciiString("ya"));
        var input = BufferUtils.fromAsciiString("foo $bar $doo\n");

        parse(input);

        var statement = statements.get(0);
        then(statement.length).isEqualTo(3);
        then(statement[0]).isEqualTo(BufferUtils.fromAsciiString("foo"));
        then(statement[1]).isEqualTo(BufferUtils.fromAsciiString("soo"));
        then(statement[2]).isEqualTo(BufferUtils.fromAsciiString("ya"));
    }

    private int parse(DirectBuffer buffer) throws CommandException {
        var context = mock(ShellContext.class);
        when(context.getVariables()).thenReturn(variables);
        when(context.getArguments()).thenReturn(args);
        return statementParser.parse(context, buffer, 0, buffer.capacity());
    }
}
