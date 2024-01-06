package com.core.platform.shell;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.io.DirectBufferChannel;
import com.core.infrastructure.log.TestLogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.BDDAssertions.then;

public class ShellTest {

    private Shell shell;
    private DirectBufferChannel outputChannel;

    @BeforeEach
    void before_each() {
        var logFactory = new TestLogFactory();
        shell = new Shell(logFactory, new MetricFactory(logFactory));
    }

    @Nested
    class VariableTests {

        @Test
        void set_variables() throws IOException {
            run("set foo bar");
            outputChannel.truncate(0);

            var actual = run("echo $foo");

            then(actual).isEqualTo("bar\n");
        }

        @Test
        void default_variables() throws IOException {
            run("set foo bar");
            run("default foo soo");
            outputChannel.truncate(0);

            var actual = run("echo $foo");

            then(actual).isEqualTo("bar\n");
        }

        @Test
        void overwrite_variables() throws IOException {
            run("set foo bar");
            run("set foo soo");
            outputChannel.truncate(0);

            var actual = run("echo $foo");

            then(actual).isEqualTo("soo\n");
        }
    }

    @Nested
    class NavigationTests {

        @BeforeEach
        void before_each() throws IOException {
            run("create /foo/bar com.core.platform.shell.ShellTest\\$ClassWithOneArg -123");
            run("create /foo com.core.platform.shell.ShellTest\\$ClassWithOneArg 456");
            outputChannel.truncate(0);
        }

        @Test
        void cd_and_get_local_value() {
            var actual = run("cd foo;value");

            then(actual).isEqualTo("456\n");
        }

        @Test
        void cd_and_get_child_value() {
            var actual = run("cd foo;bar/value");

            then(actual).isEqualTo("-123\n");
        }

        @Test
        void create_in_subdirectory() throws IOException {
            run("create /foo com.core.platform.shell.ShellTest\\$ClassWithOneArg 456");
            run("cd foo; create bar com.core.platform.shell.ShellTest\\$ClassWithOneArg -123");
            outputChannel.truncate(0);

            var actual = run("/foo/bar/value");

            then(actual).isEqualTo("-123\n");
        }
    }

    @Nested
    class CreateTests {

        @Test
        void create_in_subdirectory_first1() throws IOException {
            run("create /foo/bar com.core.platform.shell.ShellTest\\$ClassWithOneArg -123");
            run("create /foo com.core.platform.shell.ShellTest\\$ClassWithOneArg 456");
            outputChannel.truncate(0);

            var actual = run("/foo/value");

            then(actual).isEqualTo("456\n");
        }

        @Test
        void create_in_subdirectory_first2() throws IOException {
            run("create /foo/bar com.core.platform.shell.ShellTest\\$ClassWithOneArg -123");
            run("create /foo com.core.platform.shell.ShellTest\\$ClassWithOneArg 456");
            outputChannel.truncate(0);

            var actual = run("/foo/bar/value");

            then(actual).isEqualTo("-123\n");
        }

        @Test
        void get_created_object() throws CommandException {
            run("create /foo com.core.platform.shell.ShellTest\\$ClassWithOneArg -123");

            var object = (ClassWithOneArg) shell.getObject(BufferUtils.fromAsciiString("/foo"));

            then(object.value).isEqualTo(-123);
        }

        @Test
        void created_object_invoked_onRegistered() throws CommandException {
            run("create /foo/bar/me com.core.platform.shell.ShellTest\\$ClassWithOneArg -123");

            var object = (ClassWithOneArg) shell.getObject(BufferUtils.fromAsciiString("/foo/bar/me"));

            then(object.registeredPath).isEqualTo("/foo/bar/me");
        }

        @Nested
        class OneArgConstructorTests {

            @Test
            void runObject_with_exact_params() {
                var actual = run("create /foo/bar"
                        + " com.core.platform.shell.ShellTest\\$ClassWithOneArg -123");

                then(actual).isEqualTo("ClassWithOneArg{value=-123}\n");
            }

            @Test
            void runObject_with_less_params_throws_ParseError() {
                var actual = run("create /foo/bar"
                        + " com.core.platform.shell.ShellTest\\$ClassWithOneArg");

                then(actual).startsWith("com.core.platform.shell.CommandException: could not find constructor: "
                        + "class=com.core.platform.shell.ShellTest$ClassWithOneArg, specifiedParams=0\n");
            }

            @Test
            void runObject_with_more_params_throws_ParseError() {
                var actual = run("create /foo/bar"
                        + " com.core.platform.shell.ShellTest\\$ClassWithOneArg -123 456");

                then(actual).startsWith("com.core.platform.shell.CommandException: could not find constructor: "
                        + "class=com.core.platform.shell.ShellTest$ClassWithOneArg, specifiedParams=2\n");
            }
        }

        @Nested
        class TwoArgConstructorTests {

            @Test
            void run_with_exact_params() {
                var actual = run("create /foo/bar"
                        + " com.core.platform.shell.ShellTest\\$ClassWithTwoArg -123 ABC");

                then(actual).isEqualTo("ClassWithTwoArg{value1=-123, value2='ABC'}\n");
            }

            @Test
            void run_with_less_params_throws_ParseError() {
                var actual = run("create /foo/bar"
                        + " com.core.platform.shell.ShellTest\\$ClassWithTwoArg -123");

                then(actual).startsWith("com.core.platform.shell.CommandException: could not find constructor: "
                        + "class=com.core.platform.shell.ShellTest$ClassWithTwoArg, specifiedParams=1\n");
            }

            @Test
            void run_with_more_params_throws_ParseError() {
                var actual = run("create /foo/bar"
                        + " com.core.platform.shell.ShellTest\\$ClassWithTwoArg -123 ABC DEF");

                then(actual).startsWith("com.core.platform.shell.CommandException: could not find constructor: "
                        + "class=com.core.platform.shell.ShellTest$ClassWithTwoArg, specifiedParams=3\n");
            }
        }

        @Nested
        class VarArgConstructorTests {

            @Test
            void no_var_args() {
                var actual = run("create /foo/bar"
                        + " com.core.platform.shell.ShellTest\\$ClassWithVarArgs ABC");

                then(actual).isEqualTo("ClassWithVarArgs{value1='ABC', value2=[]}\n");
            }

            @Test
            void one_var_args() {
                var actual = run("create /foo/bar"
                        + " com.core.platform.shell.ShellTest\\$ClassWithVarArgs ABC -123");

                then(actual).isEqualTo("ClassWithVarArgs{value1='ABC', value2=[-123]}\n");
            }

            @Test
            void multiple_var_args() {
                var actual = run("create /foo/bar"
                        + " com.core.platform.shell.ShellTest\\$ClassWithVarArgs ABC -123 456 -789");

                then(actual).isEqualTo("ClassWithVarArgs{value1='ABC', value2=[-123, 456, -789]}\n");
            }
        }

        @Nested
        class RefVarArgConstructorTests {

            @Test
            void no_var_args() {
                var actual = run("create /foo/bar"
                        + " com.core.platform.shell.ShellTest\\$ClassWithRefVarArgs ABC");

                then(actual).isEqualTo("ClassWithRefVarArgs{value1='ABC', value2=[]}\n");
            }

            @Test
            void one_var_args() {
                run("create /foo/bar/1"
                        + " com.core.platform.shell.ShellTest\\$ClassWithOneArg -123");
                var actual = run("create /foo/bar"
                        + " com.core.platform.shell.ShellTest\\$ClassWithRefVarArgs ABC @/foo/bar/1");

                then(actual).isEqualTo("ClassWithRefVarArgs{value1='ABC', value2=[ClassWithOneArg{value=-123}]}\n");
            }

            @Test
            void multiple_var_args() {
                run("create /foo/bar/1"
                        + " com.core.platform.shell.ShellTest\\$ClassWithOneArg -123");
                run("create /foo/bar/2"
                        + " com.core.platform.shell.ShellTest\\$ClassWithOneArg 456");
                run("create /foo/bar/3"
                        + " com.core.platform.shell.ShellTest\\$ClassWithOneArg -789");
                var actual = run("create /foo/bar"
                        + " com.core.platform.shell.ShellTest\\$ClassWithRefVarArgs ABC"
                        + " @/foo/bar/1 @/foo/bar/2 @/foo/bar/3");

                then(actual).isEqualTo("ClassWithRefVarArgs{value1='ABC', value2=[ClassWithOneArg{value=-123},"
                        + " ClassWithOneArg{value=456}, ClassWithOneArg{value=-789}]}\n");
            }
        }

        @Nested
        class ClassWithReferencedArgTests {

            @Test
            void referenced_arg() throws IOException {
                run("create /foo/bar com.core.platform.shell.ShellTest\\$ClassWithOneArg -123");
                outputChannel.truncate(0);

                var actual = run(
                        "create /foo com.core.platform.shell.ShellTest\\$ClassWithRefArg 456 @/foo/bar");

                then(actual).isEqualTo("ClassWithRefArg{value1=456, value2=ClassWithOneArg{value=-123}}\n");
            }
        }

        @Nested
        class EncodableTests {

            @Test
            void run_encodable_with_exact_params() {
                var actual = run("create /foo/bar"
                        + " com.core.platform.shell.ShellTest\\$EncodableClass -123 ABC");

                then(actual).isEqualTo("value1: -123\nvalue2: ABC\n");
            }

            @Test
            void execute_encodable_with_exact_params() {
                run("create /foo/bar"
                        + " com.core.platform.shell.ShellTest\\$EncodableClass -123 ABC");

                var actual = run("/foo/bar/status");

                then(actual).isEqualTo("-123\nABC\n");
            }
        }
    }

    private String run(String command) {
        try {
            var inputChannel = new DirectBufferChannel(command.endsWith("\n") ? command : command + "\n");
            outputChannel = new DirectBufferChannel();
            shell.open(inputChannel, outputChannel, false, false, new TextShellContextHandler());
            return BufferUtils.toAsciiString(outputChannel.getBuffer(), 0, (int) outputChannel.size());
        } catch (IOException e) {
            fail("error running command", e);
            return null;
        }
    }

    private static class ClassWithOneArg implements CommandObject {

        private final int value;
        private String registeredPath;

        public ClassWithOneArg(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "ClassWithOneArg{"
                    + "value=" + value
                    + '}';
        }

        @Command
        public int getValue() {
            return value;
        }

        @Override
        public void onRegistered(String path) throws CommandException {
            this.registeredPath = path;
        }
    }

    private static class ClassWithTwoArg {

        final int value1;
        final String value2;

        public ClassWithTwoArg(int value1, String value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public String toString() {
            return "ClassWithTwoArg{"
                    + "value1=" + value1
                    + ", value2='" + value2 + '\''
                    + '}';
        }
    }

    private static class EncodableClass implements Encodable {

        final int value1;
        final String value2;

        public EncodableClass(int value1, String value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public String toString() {
            return "ClassWithTwoArg{"
                    + "value1=" + value1
                    + ", value2='" + value2 + '\''
                    + '}';
        }

        @Command
        public void status(ObjectEncoder encoder) {
            encoder.openList().number(value1).string(value2).closeList();
        }

        @Override
        public void encode(ObjectEncoder encoder) {
            encoder.openMap()
                    .string("value1").number(value1)
                    .string("value2").string(value2)
                    .closeMap();
        }
    }

    private static class ClassWithRefArg {

        final int value1;
        final ClassWithOneArg value2;

        public ClassWithRefArg(int value1, ClassWithOneArg value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public String toString() {
            return "ClassWithRefArg{"
                    + "value1=" + value1
                    + ", value2=" + value2
                    + '}';
        }
    }

    private static class ClassWithVarArgs {

        private final String value1;
        private final int[] value2;

        public ClassWithVarArgs(String value1, int... value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public String toString() {
            return "ClassWithVarArgs{"
                    + "value1='" + value1 + '\''
                    + ", value2=" + Arrays.toString(value2)
                    + '}';
        }
    }

    private static class ClassWithRefVarArgs {

        private final String value1;
        private final ClassWithOneArg[] value2;

        public ClassWithRefVarArgs(String value1, ClassWithOneArg... value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public String toString() {
            return "ClassWithRefVarArgs{"
                    + "value1='" + value1 + '\''
                    + ", value2=" + Arrays.toString(value2)
                    + '}';
        }
    }
}
