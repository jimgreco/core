package com.core.platform.activation;

import com.core.infrastructure.collections.CoreList;
import com.core.infrastructure.collections.CoreMap;
import com.core.infrastructure.collections.Deque;
import com.core.infrastructure.collections.LinkedList;
import com.core.infrastructure.log.Log;
import com.core.infrastructure.log.LogFactory;
import com.core.infrastructure.metrics.MetricFactory;

import java.util.Map;
import java.util.Objects;

/**
 * The {@code ActivatorFactory} creates {@code Activator} objects which define a dependency graph for the activation
 * of components.
 *
 * @see Activator
 * @see Activatable
 */
public class ActivatorFactory {

    private final Map<String, Activator> nameToActivator;
    private final Map<Object, Activator> activators;
    private final Log log;
    private final Deque<Activator> updateQueue;
    private final MetricFactory metricFactory;
    private boolean updating;

    /**
     * Creates a new {@code ActivationManager}.
     *
     * @param logFactory a factory to create {@code Log}s
     * @param metricFactory factory to create metrics
     */
    public ActivatorFactory(LogFactory logFactory, MetricFactory metricFactory) {
        Objects.requireNonNull(logFactory, "logFactory is null");
        this.metricFactory = Objects.requireNonNull(metricFactory, "metricFactory is null");

        log = logFactory.create(getClass());
        nameToActivator = new CoreMap<>();
        activators = new CoreMap<>();
        updateQueue = new LinkedList<>();
    }

    /**
     * Adds an activator to the manager.
     *
     * @param name the unique name of the activator
     * @param activatorObject the object that the activator represents
     * @param dependencies activator objects that are dependencies of this activator
     * @return the activator
     * @throws NullPointerException if any of the parameters are null
     * @throws IllegalArgumentException an activator with the specified {@code name} already exists, the
     *     {@code activatorObject} has already been added, or any of the dependencies have not been previously added as
     *     activators
     */
    public Activator createActivator(String name, Object activatorObject, Object... dependencies) {
        Objects.requireNonNull(name, "name is null");
        Objects.requireNonNull(activatorObject, "activatorObject is null");
        Objects.requireNonNull(dependencies, "dependencies is null");

        var children = new CoreList<Activator>();
        for (var dependency : dependencies) {
            Objects.requireNonNull(dependency, "dependency is null");
            if (dependency instanceof Activator) {
                dependency = ((Activator) dependency).getObject();
            }
            Activator dependencyActivator = getActivator(dependency);
            if (dependencyActivator == null) {
                throw new IllegalArgumentException("dependency is not an activator: " + dependency);
            }
            children.add(dependencyActivator);
        }
        if (nameToActivator.containsKey(name)) {
            throw new IllegalArgumentException("activator with name already exists: " + name);
        }
        if (activators.containsKey(activatorObject)) {
            throw new IllegalArgumentException("activator already added: " + activatorObject);
        }

        var activator = new Activator(
                log, metricFactory, this, name, activatorObject, children);
        nameToActivator.put(name, activator);
        activators.put(activatorObject, activator);
        return activator;
    }

    /**
     * Returns the activator that is associated with the specified object.
     *
     * @param object the activator object
     * @return the activator
     */
    public Activator getActivator(Object object) {
        return activators.get(object);
    }

    void update(Activator activator) {
        if (updating) {
            // queue will be exhausted in the try/finally below
            updateQueue.addLast(activator);
            return;
        }

        updating = true;
        activator.updateState();
        while (!updateQueue.isEmpty()) {
            var next = updateQueue.removeFirst();
            next.updateState();
        }
        updating = false;
    }
}
