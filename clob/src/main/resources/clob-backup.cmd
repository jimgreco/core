#
# usage: clob.cmd
# description: demonstration of a clob inside the sequencer
#

source network-local.cmd
source -s sysout-log.cmd
source -s telnet.cmd inet:0.0.0.0:7002
create /bus/schema com.core.clob.schema.ClobSchema
create /bus com.core.platform.bus.mold.MoldBusClient \
    client @/bus/schema $event_channel $command_channel $discovery_channel

# Backup Sequencer
create /busServer/store com.core.platform.bus.mold.BufferChannelMessageStore
create /busServer com.core.platform.bus.mold.MoldBusServer \
    server @/bus @/bus/schema @/busServer/store $event_channel $command_channel $discovery_channel

create seq01b com.core.platform.applications.sequencer.Sequencer @/busServer SEQ01
create seq01b/handlers com.core.clob.applications.sequencer.ClobCommandHandlers @/busServer
#

# Primary Reference Data Publisher
create ref01a com.core.platform.applications.refdata.ReferenceDataPublisher @/bus REF01 equityDefinition
ref01a/setPath clob/src/main/resources
ref01a/loadFiles
ref01a/start
#

# Backup Injector
create inject01b com.core.clob.applications.utilities.ClobInjector @/bus INJ01
#

/bus/start