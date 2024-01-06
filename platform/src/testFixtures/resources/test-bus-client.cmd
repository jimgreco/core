#
# usage: test-bus-client.cmd
# description: load a bus client that works on a loopback multicast bus
#

source test-bus.cmd
create bus com.core.platform.bus.mold.MoldBusClient @bus/schema $event_channel $command_channel $discovery_channel
