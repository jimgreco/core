package com.core.platform;

import com.core.infrastructure.buffer.BufferUtils;
import com.core.infrastructure.EventLoop;
import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.io.NioSelector;
import com.core.infrastructure.io.Selector;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.metrics.MetricFactory;
import com.core.infrastructure.time.ManualTime;
import com.core.infrastructure.time.Scheduler;
import com.core.infrastructure.time.Time;
import com.core.platform.activation.ActivatorFactory;
import com.core.platform.bus.playback.FilePlayback;
import com.core.platform.shell.CommandException;
import com.core.platform.shell.Shell;
import com.core.platform.shell.TextShellContextHandler;
import org.agrona.DirectBuffer;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;

/**
 * The main entry point for running core applications.
 *
 * <p>Main supports the following arguments to load command files.
 * <ul>
 *     <li>-s &lt;file&gt; [args ...]: loads the specified file with arguments in a sub-shell context, none of the
 *         variables set will not be brought over into new shell contexts
 *     <li>-f &lt;file&gt; [args ...]: loads the specified file with arguments in the main shell context, the variables
 *         set will be brought over into new shell contexts
 * </ul>
 *
 * <p>Command files are loaded in the order specified in the arguments.
 * If there is an error processing any command, the VM will exit.
 *
 * <p>Main sets up the following implicit parameters for shell commands.
 * These parameters are not required to be specified when shell commands are executed.
 * <table>
 *     <caption>Implicit parameters</caption>
 *     <tr>
 *         <th>Directory</th>
 *         <th>Type</th>
 *         <th>Description</th>
 *     </tr>
 *     <tr>
 *         <td>/vm/activation</td>
 *         <td>{@link ActivatorFactory}</td>
 *         <td>a factory for creating activators</td>
 *     </tr>
 *     <tr>
 *         <td>/vm/event</td>
 *         <td>{@link EventLoop}</td>
 *         <td>the event service</td>
 *     </tr>
 *     <tr>
 *         <td>/vm/log</td>
 *         <td>{@link LogFactory}</td>
 *         <td>a factory for creating logs</td>
 *     </tr>
 *     <tr>
 *         <td>/vm/metrics</td>
 *         <td>{@link MetricFactory}</td>
 *         <td>a factory for creating metrics</td>
 *     </tr>
 *     <tr>
 *         <td>/vm/scheduler</td>
 *         <td>{@link Scheduler}</td>
 *         <td>a system task scheduler that is fired through the event service</td>
 *     </tr>
 *     <tr>
 *         <td>/vm/selector</td>
 *         <td>{@link Selector}</td>
 *         <td>the selector for creating I/O components that operate off the event service</td>
 *     </tr>
 *     <tr>
 *         <td>/vm/shell</td>
 *         <td>{@link Shell}</td>
 *         <td>the shell for creating new contexts</td>
 *     </tr>
 *     <tr>
 *         <td>/vm/time</td>
 *         <td>@link Time}</td>
 *         <td>a source of system timestamps</td>
 *     </tr>
 * </table>
 *
 * <p>Main can operate in a playback mode if a {@code corefile} property is specified with the path of the corefile to
 * run.
 * This will load a {@link ManualTime} implementation of the time source and execute the file using a
 * {@link FilePlayback} object that is instantiated by the commands file.
 */
public class Main {

    private final Shell shell;
    private final EventLoop eventLoop;

    /**
     * Starts a core VM program.
     * See the class description for a description of the program arguments.
     *
     * @param args the arguments to the program
     * @throws IOException if a specified file could not be found
     * @throws CommandException if there is an error processing a command
     */
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws IOException, CommandException {
        var coreFile = System.getProperty("corefile");

        var time = coreFile == null ? Time.createSystemTime() : new ManualTime();
        var logFactory = new LogFactory(time);
        var vmName = System.getProperty("vm_name", "vm");
        System.setProperty("vm_name", vmName);
        var coreName = System.getProperty("core_name", "core");
        System.setProperty("core_name", coreName);
        logFactory.setLogIdentifier(0, vmName);

        var main = new Main(time, logFactory);

        main.processArgs(args);

        if (coreFile == null) {
            main.run();
        } else {
            // TODO: a hard-coded path is a little hacky
            var shell = main.getShell();
            var playback = (Consumer<String>) shell.getObject(BufferUtils.fromAsciiString("/playback"));
            playback.accept(coreFile);
        }
    }

    /**
     * Construct an instance of The Core event loop runner.
     *
     * @param time the time instance
     * @param logFactory the log factory
     * @throws IOException from create NioSelector
     * @throws CommandException for Shell errors
     */
    protected Main(Time time, LogFactory logFactory) throws IOException, CommandException {
        var scheduler = new Scheduler(time);
        var selector = new NioSelector();
        selector.setScheduler(scheduler);
        eventLoop = new EventLoop(time, scheduler, selector);

        var metricFactory = new MetricFactory(logFactory);
        shell = new Shell(logFactory, metricFactory);
        shell.addVariables(System.getProperties());
        shell.setPropertyValue("version", System.getProperty("version", "development"));

        var activatorFactory = new ActivatorFactory(logFactory, metricFactory);
        shell.setImpliedParameter(ActivatorFactory.class, activatorFactory);
        shell.setImpliedParameter(EventLoop.class, eventLoop);
        shell.setImpliedParameter(LogFactory.class, logFactory);
        shell.setImpliedParameter(MetricFactory.class, metricFactory);
        shell.setImpliedParameter(Scheduler.class, scheduler);
        shell.setImpliedParameter(Selector.class, selector);
        shell.setImpliedParameter(Shell.class, shell);
        shell.setImpliedParameter(Time.class, time);

        shell.addObject(BufferUtils.fromAsciiString("/vm/activation"), activatorFactory);
        shell.addObject(BufferUtils.fromAsciiString("/vm/event"), eventLoop);
        shell.addObject(BufferUtils.fromAsciiString("/vm/log"), logFactory);
        shell.addObject(BufferUtils.fromAsciiString("/vm/metrics"), metricFactory);
        shell.addObject(BufferUtils.fromAsciiString("/vm/scheduler"), scheduler);
        shell.addObject(BufferUtils.fromAsciiString("/vm/selector"), selector);
        shell.addObject(BufferUtils.fromAsciiString("/vm/shell"), shell);
        shell.addObject(BufferUtils.fromAsciiString("/vm/time"), time);
    }

    /**
     * Return the current shell.
     *
     * @return the shell
     */
    public Shell getShell() {
        return shell;
    }

    /**
     * Run the event loop.
     *
     * @throws IOException from event loop select calls
     * @throws CommandException if command path can not be resolved
     */
    public void run() throws IOException, CommandException {
        var runner = shell.getPropertyValue(BufferUtils.temp("runner"));
        if (runner == null) {
            eventLoop.run();
        } else {
            ((Runnable) shell.getObject(runner)).run();
        }
    }

    private void processArgs(String[] args) throws IOException, CommandException {
        var context = shell.open(null, null, false, true, new TextShellContextHandler());

        var startOfFileIndex = -1;
        for (var i = 0; i < args.length; i++) {
            var subShell = "-s".equals(args[i]);
            var file = "-f".equals(args[i]);
            var last = i == args.length - 1;
            if (last) {
                i++;
            }
            if (subShell || file || last) {
                if (startOfFileIndex == -1) {
                    startOfFileIndex = i + 1;
                } else if (startOfFileIndex == i) {
                    throw new IOException("empty option: args=" + Arrays.toString(args) + ", index=" + i);
                } else {
                    var path = BufferUtils.fromAsciiString(args[startOfFileIndex]);
                    var argsList = new CoreList<DirectBuffer>();
                    for (var j = startOfFileIndex; j < i; j++) {
                        argsList.add(BufferUtils.fromAsciiString(args[j]));
                    }
                    context.loadFile(path, argsList, subShell);
                    context.close();
                    startOfFileIndex = -1;
                }
            }
        }
    }
}
