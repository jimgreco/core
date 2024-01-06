#
# usage: vm01.cmd
# description: primary sequencer
#

source -s sysout-log.cmd
source -s telnet.cmd inet:0.0.0.0:7001

create bus/schema com.core.platform.schema.TestSchema
source test-bus.cmd

create busServer/store com.core.platform.bus.mold.BufferChannelMessageStore
create busServer com.core.platform.bus.mold.MoldBusServer @busServer/schema @busServer/store $event_channel $command_channel $discovery_channel
busServer/createSession AA

create seq01a com.core.platform.applications.sequencer.Sequencer @busServer seq01a
seq01a/start
