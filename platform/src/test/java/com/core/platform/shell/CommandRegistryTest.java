package com.core.platform.shell;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.command.Directory;
import com.core.infrastructure.log.TestLogFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;

public class CommandRegistryTest {

    private CommandRegistry registry;
    private HashMap<Class<?>, Object> impliedParams;

    @BeforeEach
    void before_each() {
        impliedParams = new HashMap<>();
        registry = new CommandRegistry(new TestLogFactory(), impliedParams);
    }

    @Nested
    class NavigateTests {

        @Test
        void navigate_to_root_from_null() throws CommandException {
            var actual = registry.resolve(registry.getRoot(), BufferUtils.fromAsciiString("/"));

            then(actual.toString()).isEqualTo("/");
        }

        @Test
        void navigate_to_root_from_root() throws CommandException {
            var expected = registry.resolve(registry.getRoot(), BufferUtils.fromAsciiString("/"));

            var actual = registry.resolve(expected, BufferUtils.fromAsciiString("/"));

            then(actual).isSameAs(expected);
        }

        @Test
        void create_a_subdirectory_from_null() throws CommandException {
            var actual = registry.resolve(registry.getRoot(), BufferUtils.fromAsciiString("foo"), true);

            then(actual.toString()).isEqualTo("/foo");
        }

        @Test
        void create_subdirectories_from_null() throws CommandException {
            var actual = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("foo/bar/me"), true);

            then(actual.toString()).isEqualTo("/foo/bar/me");
        }

        @Test
        void create_subdirectories_from_directory() throws CommandException {
            var subdirectory = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("foo/bar"), true);

            var actual = registry.resolve(
                    subdirectory, BufferUtils.fromAsciiString("me/soo"), true);

            then(actual.toString()).isEqualTo("/foo/bar/me/soo");
        }

        @Test
        void create_subdirectories_from_root() throws CommandException {
            var subdirectory = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("foo/bar"), true);

            var actual = registry.resolve(
                    subdirectory, BufferUtils.fromAsciiString("/me/soo"), true);

            then(actual.toString()).isEqualTo("/me/soo");
        }

        @Test
        void navigate_to_parent_directory() throws CommandException {
            var subdirectory = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("foo/bar"), true);

            var actual = registry.resolve(subdirectory, BufferUtils.fromAsciiString(".."));

            then(actual.toString()).isEqualTo("/foo");
        }

        @Test
        void navigate_to_grandparent_directory() throws CommandException {
            var subdirectory = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("foo/bar"), true);

            var actual = registry.resolve(
                    subdirectory, BufferUtils.fromAsciiString("../.."));

            then(actual.toString()).isEqualTo("/");
        }

        @Test
        void navigate_beyond_root_throws_ParseError() throws CommandException {
            var subdirectory = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("foo/bar"), true);

            thenThrownBy(() -> registry.resolve(subdirectory, BufferUtils.fromAsciiString("../../..")))
                    .isInstanceOf(CommandException.class);
        }

        @Test
        void navigate_to_unknown_directory_throws_ParseError() throws CommandException {
            var subdirectory = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("foo/bar"), true);

            thenThrownBy(() -> registry.resolve(subdirectory, BufferUtils.fromAsciiString("soo")))
                    .isInstanceOf(CommandException.class);
        }

        @Test
        void navigate_with_relative_path() throws CommandException {
            var subdirectory = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("foo/bar"), true);

            var actual = registry.resolve(
                    subdirectory, BufferUtils.fromAsciiString("./soo/./goo"), true);

            then(actual.toString()).isEqualTo("/foo/bar/soo/goo");
        }

        @Test
        void navigate_to_current_directory() throws CommandException {
            var subdirectory = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("foo/bar"), true);

            var actual = registry.resolve(
                    subdirectory, BufferUtils.fromAsciiString("."));

            then(actual.toString()).isEqualTo("/foo/bar");
        }

        @Test
        void navigate_to_parent_directory_only() throws CommandException {
            var subdirectory = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("foo/bar"), true);

            var actual = registry.resolve(subdirectory, BufferUtils.fromAsciiString(".."));

            then(actual.toString()).isEqualTo("/foo");
        }
    }

    @Nested
    class AddCommandsTests {

        private ClassWithCommands obj;

        @BeforeEach
        void before_each() throws CommandException {
            var subdirectory = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("foo/bar"), true);
            obj = new ClassWithCommands();
            registry.addObject(subdirectory, BufferUtils.fromAsciiString("me"), obj);
        }

        @Test
        void execute_set_command() throws Throwable {
            var commandDir = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("/foo/bar/me/setFoo"));

            commandDir.execute(List.of(123));

            then(obj.foo).isEqualTo(123);
        }

        @Test
        void execute_get_command() throws Throwable {
            var commandDir = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("/foo/bar/me/setFoo"));
            commandDir.execute(List.of(123));
            commandDir = registry.resolve(registry.getRoot(), BufferUtils.fromAsciiString("/foo/bar/me/foo"));

            var actual = commandDir.execute(List.of());

            then(actual).isEqualTo(123);
        }

        @Test
        void execute_varArgs_command() throws Throwable {
            var commandDir = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("/foo/bar/me/setBar"));

            commandDir.execute(List.of(123, "soo", "me"));

            then(obj.foo).isEqualTo(123);
            then(obj.bar).isEqualTo(new String[] { "soo", "me" });
        }

        @Test
        void param_types() throws Throwable {
            var commandDir = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("/foo/bar/me/setBar"));

            var paramTypes = commandDir.getParameterTypes();

            then(commandDir.isVarArgs()).isTrue();
            then(paramTypes.length).isEqualTo(2);
            then(paramTypes[0]).isEqualTo(int.class);
            then(paramTypes[1]).isEqualTo(String[].class);
        }

        @Test
        void execute_command_with_another_name() throws Throwable {
            var commandDir = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("/foo/bar/me/another"));

            var actual = commandDir.execute(List.of());

            then(actual).isEqualTo(17);
        }

        @Test
        void execute_included_with_local_path_command() throws Throwable {
            var commandDir = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("/foo/bar/me/it"));

            var actual = commandDir.execute(List.of());

            then(actual).isEqualTo(1);
        }

        @Test
        void execute_included_with_path_command() throws Throwable {
            var commandDir = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("/foo/bar/me/inners/inner2/it"));

            var actual = commandDir.execute(List.of());

            then(actual).isEqualTo(1);
        }

        @Test
        void execute_included_no_path_command() throws Throwable {
            var commandDir = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("/foo/bar/me/inner3/it"));

            var actual = commandDir.execute(List.of());

            then(actual).isEqualTo(1);
        }

        @Test
        void properties_read_write() throws Throwable {
            registry.addObject(registry.getRoot(), BufferUtils.fromAsciiString("/soo"), new PropertyClass());
            var getterCommand = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("/soo/value"));
            var setterCommand = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("/soo/setValue"));
            setterCommand.execute(List.of(123));

            var actual = getterCommand.execute(List.of());

            then(actual).isEqualTo(123);
        }

        @Test
        void properties_write_only() throws Throwable {
            registry.addObject(registry.getRoot(), BufferUtils.fromAsciiString("/soo"), new PropertyClass());
            var property = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("/soo/setValue2"));

            property.execute(List.of(123));

            var obj = registry.resolve(registry.getRoot(), BufferUtils.fromAsciiString("/soo"));
            then(((PropertyClass) obj.getObject()).value2).isEqualTo(123);
        }

        @Test
        void properties_read_only() throws Throwable {
            registry.addObject(registry.getRoot(), BufferUtils.fromAsciiString("/soo"), new PropertyClass());
            var property = registry.resolve(
                    registry.getRoot(), BufferUtils.fromAsciiString("/soo/value3"));
            var obj = registry.resolve(registry.getRoot(), BufferUtils.fromAsciiString("/soo"));
            ((PropertyClass) obj.getObject()).value3 = 123;

            var actual = property.execute(List.of());

            then(actual).isEqualTo(123);
        }
    }

    @SuppressWarnings("PMD")
    public static class ClassWithCommands {

        private int foo;
        private String[] bar;

        @Directory(path = ".")
        private InnerClass inner1;
        @Directory(path = "inners/inner2")
        private InnerClass inner2;
        @Directory
        private InnerClass inner3;

        public ClassWithCommands() {
            inner1 = new InnerClass();
            inner2 = new InnerClass();
            inner3 = new InnerClass();
        }

        @Command(path = "setFoo")
        void setFoo(int foo) {
            this.foo = foo;
        }

        @Command
        private int getFoo() {
            return foo;
        }

        @Command
        public void setBar(int foo, String... bar) {
            this.foo = foo;
            this.bar = bar;
        }

        @Command(path = "another")
        public int getAnother() {
            return 17;
        }
    }

    public static class InnerClass {

        @Command
        public int getIt() {
            return 1;
        }
    }

    @SuppressWarnings("PMD.UnusedPrivateField")
    public static class PropertyClass {

        @Property(write = true)
        private int value;

        @Property(read = false, write = true)
        private int value2;

        @Property
        private int value3;
    }
}
