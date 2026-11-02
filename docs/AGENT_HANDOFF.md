# Core trading platform — coding handoff

Snapshot: **2026-11-02 UTC**. This is a takeover map, not release authorization. Recheck Git status, the requested scope and current runtime before acting. Existing AGENTS.md and linked contract/runbooks remain applicable. Older dated status notes are evidence from their stated date, not current permissions.

## Purpose and architecture

Historical Java infrastructure/platform used for a sequenced trading system, with a sample central limit order book.

Gradle8.5 multi-module Java17: buildSrc binary/FIX generators, infrastructure allocation-free IO/buffers, platform shell/sequencer/apps, and clob domain example. Agrona/Eclipse Collections support low-allocation operation.

## Locations and current work

- Usual local workspace: `/Users/jgreco/Documents/core`.
- Source inspected for this handoff: `/Users/jgreco/Documents/core`.
- Existing repository: [jimgreco/core](https://github.com/jimgreco/core); default branch `main`, verified source head `3f2b5c38d84cf1841f9ac8320d51257707a02819`.
- Documentation branch: `docs/agent-handoff-2026-11-02`, based on that head; application code was not changed.
- Primary source HEAD matched the verified remote at inventory time; branch `main`.

## Code and guide map

- [buildSrc](../buildSrc)
- [infrastructure](../infrastructure)
- [platform](../platform)
- [clob](../clob)
- [config](../config)
- [settings.gradle](../settings.gradle)

Read the relevant existing guide before changing its contract:

- [README.md](../README.md)

## Local setup and dependencies

Commands below are source-verified entry points, **not commands executed during this documentation pass**. Use disposable local data. Read the command comments and linked setup guide before running migrations, creating fixtures or starting listeners. Reuse prepared environments where suitable.

- `Use JDK17 and the checked-in Gradle wrapper.`
- `./gradlew classes testClasses  # first dependency resolution may require network`

Configuration names/locations only; never copy secret values into source, terminal output or handoff documents:

- `JAVA_HOME`
- `SHELL_PATH`
- `gradle.properties`

Use existing lockfiles/manifests; do not upgrade dependencies to perform a handoff. For native work, select an actually installed Xcode SDK/simulator from the checked-in project configuration.

## Tests and verification

- `./gradlew test`
- `./gradlew check  # includes configured Checkstyle/PMD`
- `Review fixtures before running network-oriented integration programs.`

Documentation-only changes require path/link, accuracy, diff and secret-exclusion review; do not run heavyweight builds or production smoke mutations for prose changes. Application tests were not rerun in this pass. Past test/release evidence below retains its original date. UI changes still require actual rendered inspection, and synthetic tests do not prove real provider/device acceptance.

## Decisions and constraints

- Command files create and wire objects; shell transports can mutate the running system. Treat historical network/config examples as examples, not production endpoints.
- Preserve allocation/message-layout and sequencing contracts when changing internals.

## Release state and workflow effects

Main3f2b5c3 verified against GitHub; no live trading system or deployment inspected.

No exact-head GitHub run was present in the bounded recent-main query. This is not proof of deployment, build success or absence of older runs.

The handoff is published on a documentation feature branch, not main. Current checked-in GitHub push triggers are main-only (or absent). Blog hosting branch settings were separately checked. No native release, image publication or server deployment is intended from this documentation push.

## Known gaps and next safe work

- No current CI/deployment found. Modern Mac/JDK build was not executed during documentation. Resolve actual build failures before claiming compatibility.

Before the next task, verify source HEAD, dirty/staged work and the selected subsystem; read only the relevant guide. Check whether a listed idea is already implemented in another branch before rebuilding it. Preserve unrelated work and paused experiments. Release/permission/data changes require the applicable current authorization.
