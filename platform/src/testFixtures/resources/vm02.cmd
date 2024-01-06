#
# usage: vm02.cmd
# description: reference data publisher
#

source -s sysout-log.cmd
source -s telnet.cmd inet:0.0.0.0:7002

create bus/schema com.core.platform.schema.TestSchema
source test-bus-client.cmd

create ref01a com.core.platform.applications.refdata.ReferenceDataPublisher @/bus REF01A currency spot
ref01a/setPath platform/src/testFixtures/resources
ref01a/start
ref01a/loadFiles
