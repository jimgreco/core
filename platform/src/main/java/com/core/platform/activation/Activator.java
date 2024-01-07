package com.core.platform.activation;

import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.command.Command;
import com.core.infrastructure.command.Property;
import com.core.infrastructure.encoding.Encodable;
import com.core.infrastructure.encoding.ObjectEncoder;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.metrics.MetricFactory;

import java.util.List;

/**
 * Activators are the nodes in the activation dependency graph.
 *
 * <p>Activators are created with no parents and optionally with child activator dependencies.
 * After an activator is created, it can become the dependency of another activator.
 *
 * <p>Each activator has the following independent state variables.
 * <ul>
 *     <li>Ready: the object is ready
 *     <li>Started: the object has been started
 *     <li>Activating: the activator is in the process of going active
 *     <li>Deactivating: the activator is in process of going inactive
 *     <li>Active: the activator is ready and started
 * </ul>
 *
 * <p>To be set active, the activator must be ready (by invoking {@link #ready()} on the activator), started (by
 * invoking {@link #start()} on the activator), and all the child dependencies must be active.
 * To be set inactive after being set active, the activator can be set not ready (by invoking {@link #notReady()},
 * stopped (by invoking {@link #stop()} on the activator or the parent's activator invoking {@code stop} on the
 * activator), or any of the child dependencies become inactive.
 *
 * <p>When {@link #start()} is invoked, {@code start} is invoked on all descendant activators if not already started.
 * Similarly, when {@link #stop()} is invoked, {@code stop} is invoked on all descendant activators not not already
 * stopped.
 * This allows a parent object (e.g., an application) to start all its dependencies.
 *
 * <p>Activators are created with an associated object.
 * If the associated object is an {@link Activatable} then {@link Activatable#activate()} is invoked on the object when
 * the activator is started and all child activators are active.
 * This gives the object an opportunity to do some initialization, knowing that all dependencies are satisfied.
 * The associated object does not need to invoke {@link #ready()} on the activator during the {@code activate}
 * method call, but can be done at a later time (e.g., after a FIX engine Logon[A]s).
 *
 * <p>Similarly, when the activator is stopped or one of the children goes inactive, then
 * {@link Activatable#deactivate()} is invoked on the object.
 * The associated object does not need to invoke {@link #notReady()} on the activator during the {@code deactivate}
 * invocation, but can do so at a later time (e.g., after a FIX engine disconnects).
 *
 * @see ActivatorFactory
 * @see Activatable
 */
public class Activator implements Comparable<Activator>, Encodable {

    private final String name;
    private final Object activatorObject;
    @Property
    private final List<Activator> children;
    @Property
    private final List<Activator> parents;
    private final Log log;
    private final ActivatorFactory activationFactory;

    private boolean active;
    private boolean activating;
    private boolean deactivating;
    private boolean started;
    private boolean ready;
    private String notReadyReason;

    private boolean pendingStarted;
    private boolean pendingStopped;
    private boolean pendingReady;
    private boolean pendingNotReady;
    private String pendingNotReadyReason;
    private boolean preventParentStop;

    Activator(Log log, MetricFactory metricFactory, ActivatorFactory activatorFactory, String name,
              Object activatorObject, List<Activator> children) {
        this.log = log;
        this.activationFactory = activatorFactory;
        this.name = name;
        this.activatorObject = activatorObject;
        this.children = children;
        parents = new CoreList<>();
        for (var child : children) {
            child.parents.add(this);
        }
        notReadyReason = "initial state";

        metricFactory.registerSwitchMetric("Activator_Active", this::isActive, "name", name);
        metricFactory.registerSwitchMetric("Activator_Started", this::isStarted, "name", name);
        metricFactory.registerSwitchMetric("Activator_Ready", this::isReady, "name", name);
        metricFactory.registerStateMetric("Activator_Reason", this::getNotReadyReason, "name", name);
    }

    /**
     * Returns the object associated with the activator.
     *
     * @return the object associated with the activator
     */
    public Object getObject() {
        return activatorObject;
    }

    /**
     * Returns true if the activator is active.
     *
     * @return true if the activator is active
     */
    public boolean isActive() {
        return active;
    }

    /**
     * Returns true if the activator is activating.
     *
     * @return true if the activator is activating
     */
    public boolean isActivating() {
        return activating;
    }

    /**
     * Returns true if the activator is deactivating.
     *
     * @return true if the activator is deactivating
     */
    public boolean isDeactivating() {
        return deactivating;
    }

    /**
     * Returns true if the activator is started.
     *
     * @return true if the activator is started
     */
    public boolean isStarted() {
        return started;
    }

    /**
     * Returns true if the activator is ready.
     *
     * @return true if the activator is ready
     */
    public boolean isReady() {
        return ready;
    }

    /**
     * Returns the most recent reason the activator is not ready.
     *
     * @return the most recent reason the activator is not ready
     */
    public String getNotReadyReason() {
        return notReadyReason;
    }

    /**
     * Sets the activator to the ready state.
     * If the object associated with this activator is an {@code Activatable}, the activator is started, and all
     * descendants are active then {@link Activatable#deactivate() activate} is invoked on the object.
     */
    public void ready() {
        pendingReady = true;
        pendingNotReady = false;
        pendingNotReadyReason = null;
        activationFactory.update(this);
    }

    /**
     * Sets the activator to the not ready state with the reason "deactivated".
     * This activator and all ancestors will be set inactive.
     * If the object associated with this activator is an {@code Activatable} then
     * {@link Activatable#deactivate() deactiave} is invoked on the object.
     */
    public void notReady() {
        notReady("deactivated");
    }

    /**
     * Sets the activator to the not ready state for the specified reason.
     * This activator and all ancestors will be set inactive.
     * If the object associated with this activator is an {@code Activatable} then
     * {@link Activatable#deactivate() deactiave} is invoked on the object.
     *
     * @param reason the reason the activator is not ready
     */
    public void notReady(String reason) {
        pendingReady = false;
        pendingNotReady = true;
        pendingNotReadyReason = reason;
        activationFactory.update(this);
    }

    /**
     * Prevents a stopping of this activator if all parents are stopped.
     * This is useful for components you always want to run unless shutdown explicitly.
     */
    public void preventParentStop() {
        preventParentStop = true;
    }

    /**
     * Sets the activator to the started state.
     * If the object associated with this activator is an {@code Activatable}, the activator is ready, and all
     * descendants are active then {@link Activatable#deactivate() activate} is invoked on the object.
     */
    @Command
    public void start() {
        pendingStarted = true;
        pendingStopped = false;
        activationFactory.update(this);
    }

    /**
     * Sets the activator to the stopped state.
     * This activator and all ancestors will be set inactive.
     * If the object associated with this activator is an {@code Activatable} then
     * {@link Activatable#deactivate() deactiave} is invoked on the object.
     */
    @Command
    public void stop() {
        pendingStarted = false;
        pendingStopped = true;
        activationFactory.update(this);
    }

    void updateState() {
        updatePendingState();
        if (!isActive() && isStarted() && childrenActive()) {
            makeActive();
        } else if ((isActivating() || isActive())
                && (!isStarted() || !isReady() || !childrenActive())) {
            makeInactive();
        }
    }

    private boolean parentsStarted() {
        var parentsStarted = false;
        for (var parent : parents) {
            parentsStarted |= parent.isStarted() || parent.parentsStarted();
        }
        return parentsStarted;
    }

    private boolean childrenActive() {
        var childrenActivated = true;
        for (var child : children) {
            childrenActivated &= child.isActive() && child.childrenActive();
        }
        return childrenActivated;
    }

    private void updatePendingState() {
        if (pendingReady && !ready) {
            // activated
            log.info().append("not ready => ready: ").append(name).commit();
            ready = true;
            notReadyReason = null;
            pendingReady = false;
        } else if (pendingNotReady && ready) {
            // deactivated
            log.info().append("ready => not ready: name=").append(name)
                    .append(", reason=").append(pendingNotReadyReason)
                    .commit();
            ready = false;
            notReadyReason = pendingNotReadyReason;
            pendingNotReady = false;
        } else if (pendingNotReady && !pendingNotReadyReason.equals(notReadyReason)) {
            // deactivated reason change
            log.info().append("not ready reason change: name=").append(name)
                    .append(", reason=").append(pendingNotReadyReason)
                    .append(", oldReason=").append(notReadyReason)
                    .commit();
            ready = false;
            notReadyReason = pendingNotReadyReason;
            pendingNotReady = false;
        }

        if (pendingStarted && !started) {
            // setting active
            log.info().append("not started => started: ").append(name).commit();
            started = true;
            pendingStarted = false;
            for (var child : children) {
                child.start();
            }
        } else if (pendingStopped && started) {
            // setting inactive
            log.info().append("started => not started: ").append(name).commit();
            started = false;
            pendingStopped = false;
            for (var child : children) {
                if (!child.parentsStarted() && !child.preventParentStop) {
                    child.stop();
                }
            }
        }
    }

    private void makeActive() {
        if (isReady()) {
            active = true;
            activating = false;
            log.info().append("active: ").append(name).commit();
            updateParents();
        } else if (!isActivating() && activatorObject instanceof Activatable) {
            try {
                activating = true;
                log.info().append("activating: ").append(name).commit();
                ((Activatable) activatorObject).activate();
            } catch (Exception e) {
                log.warn().append("error in activating activator object, stopping: name=").append(name)
                        .append(", exception=").append(e)
                        .commit();
                stop();
            }
        }
    }

    private void makeInactive() {
        active = false;
        activating = false;
        deactivating = true;
        if (activatorObject instanceof Activatable) {
            try {
                log.info().append("deactivating: ").append(name).commit();
                ((Activatable) activatorObject).deactivate();
            } catch (Exception e) {
                log.warn().append("error in deactivating activator object: name=").append(name)
                        .append(", exception=").append(e)
                        .commit();
            }
        }
        deactivating = false;
        updateParents();
    }

    private void updateParents() {
        for (var parent : parents) {
            activationFactory.update(parent);
        }
    }

    @Override
    public String toString() {
        return toEncodedString();
    }

    @Command(path = "state", readOnly = true)
    @Override
    public void encode(ObjectEncoder encoder) {
        encoder.openMap()
                .string("name").string(name)
                .string("active").bool(active);
        if (!active) {
            encoder.string("activating").bool(activating)
                    .string("deactivating").bool(deactivating)
                    .string("started").bool(started)
                    .string("ready").bool(ready)
                    .string("notReadyReason").string(notReadyReason)
                    .string("children").openList();
            for (var child : children) {
                encoder.object(child);
            }
            encoder.closeList();
        }
        encoder.closeMap();
    }

    @Override
    public int compareTo(Activator o) {
        return o.name.compareTo(name);
    }
}
