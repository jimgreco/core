# QuickStart Guide

## Project Structure

This project contains the infrastructure and platform that was used to build the F9 proprietary trading system.

The codebase is divided into four modules:

- `buildSrc` code generators for binary encoders/decoders and FIX messages.
- `infrastructure` low-level allocation-free utilities for async I/O and buffer processing.
- `platform` the Sequencer architecture and applications common to all cores.
- `clob` a demo of how to build a trading core with domain-specific knowledge on top of the platform

The project has only two dependencies: Agrona for a byte buffer abstraction and Eclipse collections for garbage-free collections.

## Command Shell

The core system is designed around a command shell.
All VM infrastructure and each core application is bootstrapped through the command shell with the execution of commands.
Commands can create objects, register objects in a directory tree structure (similar to a file system), and execute methods on registered objects. 
See `com.core.platform.shell.Shell` for a full description of the shell's built-in commands.

There are multiple ways to execute commands:
1. From a commands file when the VM is being initialized
2. Through a Telnet session, HTTP REST command, or WebSocket session after the VM is initialized

### Commands Files

Commands files are loaded when the VM is being initialized. The VM will exit if all commands in the file are not executed successfully.

The following example creates and initialized the printer application.

    set log_directory /tmp 
    create print01a com.core.platform.applications.printer.Printer @/bus $log_directory
    print01a/deny marketData

**Line 1:** `set` is a built-in command to initialize a variable.
`set` takes two arguments, a variable name (i.e., `log_directory`) and value (`/tmp`).
The value of the variable can be expanded in subsequent commands by prefacing the name of the variable with a dollar sign (i.e., `$log_directory` will be replaced with `/tmp`).

    set log_directory /tmp

**Line 2:** `create` invokes a constructor and creates an instance of the specified class at the specified shell directory location.
`create` takes at least two arguments, the directory to register the object and the classname of the object.
Greater than two arguments are passed as parameters to the constructor.

    create print01a com.core.platform.applications.printer.Printer @/bus $log_directory

The directory the object will be stored in is is `/print01a`. 
Directories can be specified as an absolute directory or directory relative to the current directory.
The path of the class to create is `com.core.platform.applications.printer.Printer`.
The remaining arguments are passed to constructor.
If a class contains multiple constructors, the constructor will the matching number of parameters is used.

    Printer(LogFactory logFactory, BusClient<?, ?> busClient, String logDirectory) 

Parameters can reference registered objects by prefacing the directory of the variable with an at sign (i.e., `@/bus`)
Parameters can reference variables by prefacing the variable name with a dollar sign (i.e., `$log_directory`)
The last argument can be of variable arity.

There are three constructor parameters, but only two parameters are passed in.
The shell has the concept of *implicit* parameters that do not need to be passed in.
Implicit parameters are initialized when the VM is created (see the `com.core.platform.Main` class description for a full list of implicit parameters).

**Line 3:** When an object is registered, it is searched recursively for fields and methods with command annotations including, `@Command`, `@Property`, and `@Directory`.
Commands can be executed on registered objects by specifying the full path to the command and any explicit arguments required.

    print01a/deny marketData

The `com.core.platform.applications.printer.Printer` defines a method, `deny`, that is annotated with `@Command`, that takes a single string argument.

    @Command
    void deny(String name)

Since the printer application is registered at `print01a`, the command is registered in a subdirectory with the name of the method, `print01a/deny`. 

### Telnet Shell

Commands files can initialize a Telnet interface to interact with the command shell.
The Telnet shell will listen for clients on the address specified when it is initialized.
By connecting to the shell, users can navigate the registered directories and execute commands using the same syntax as the commands file.

    jgreco@Jims-MacBook-Air core % nc 0.0.0.0 7001
    / % cd print01a
    /print01a % ls
    key [String entityName] [int primaryKey]
    primaryKey [String entityName] [List key]
    suppressed
    disabled
    setDisabled [boolean disabled] w
    status
    allow [String name] w
    deny [String name] w
    /print01a % deny marketData

### Main

The core has a single application entry point, `com.core.platform.Main`, in the `platform` module.
A secondary entry point, `com.core.clob.ClobMain`, can be used for running applications inside the IDE.
The following will launch a VM and load a commands file, `vm01.cmd`.

    ./gradlew uberjar
    java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -DSHELL_PATH=infrastrucrture/src/main/resources:platform/src/main/resources:clob/src/main/resources -jar clob/build/libs/core-1.0-SNAPSHOT.jar com.core.platform.Main -s clob.cmd 

* The `--add-opens` argument is used to enable the high-precision timer. If left out, the application will fall back to a millisecond-precision timer.
* Each `-D<variable name>=<variable value>` argument is available as a system property.
`SHELL_PATH` is the sole required property, defining the path to the commands files (similar to a UNIX shell path).
* Each `-s <file> [params ...]` loads and executes all commands in the specified commands file.

