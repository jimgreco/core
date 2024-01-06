#
# usage: vm03.cmd
# description: sequencer + reference data publisher + printer
#

# VM Infra
source -s sysout-log.cmd
source -s telnet.cmd inet:0.0.0.0:7001
create bus/schema com.core.platform.schema.TestSchema
source test-bus-client.cmd

# Printer
create print01a com.core.platform.applications.printer.Printer @bus @/sys/out

# Reference Data Publisher
create ref01a com.core.platform.applications.refdata.ReferenceDataPublisher \
    @bus REF01A currency spot
ref01a/setPath platform/src/testFixtures/resources
ref01a/start
ref01a/loadFiles

# Sequencer
create busServer/store com.core.platform.bus.mold.BufferChannelMessageStore
create busServer com.core.platform.bus.mold.MoldBusServer @bus/schema @busServer/store $event_channel $command_channel $discovery_channel
busServer/createSession AA
create seq01a com.core.platform.applications.sequencer.Sequencer @busServer seq01a
create seq01a/refData com.core.platform.applications.sequencer.CommandHandlers @busServer
seq01a/start
