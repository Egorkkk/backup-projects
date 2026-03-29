# Policy

This document describes the accepted baseline file-selection policy for `backup-projects` v1. It focuses on the current default behavior for extension allowlisting, oversize `.aaf` handling, autosave inclusion, cache exclusion, and manual include overrides.

## Allowed Extensions

The current default allowlist is:

- `prproj`
- `avb`
- `avp`
- `aep`
- `aepx`
- `drp`
- `drt`
- `edl`
- `xml`
- `fcpxml`
- `aaf`

Extensions are stored without leading dots.

Files outside this allowlist are treated as unknown extensions and are not included by default in manifest output.

## .aaf Size Policy

The current default `.aaf` size limit is `104857600` bytes (`100 MB`).

The current default oversize action for `.aaf` is `skip`.

Skipped oversized `.aaf` files still produce an oversize warning in the baseline configuration because skipped-oversize logging is enabled in `config/rules.example.yaml`.

## Autosave Inclusion

Autosave files are included when they use an allowed extension and are not excluded by the cache rules.

This baseline does not depend on a separate explicit default autosave whitelist in the rules config. In the current accepted repo behavior, autosave inclusion follows the normal extension allowlist and exclusion flow.

## Cache Exclusion

The current default cache exclusions are:

- directory names: `Cache`, `Render Cache`, `Media Cache`, `Preview Files`
- glob: `**/.cache/**`

These exclusions are applied during policy evaluation and manifest decision building.

Cache-like files can still appear in raw inventory or structural scan results before policy exclusion is applied to final include/skip decisions.

## Manual Include Overrides

Manual includes exist as registry records, but override behavior is specifically tied to `force_include`.

The accepted current behavior is:

- `force_include` can override exclude matches
- `force_include` can override unknown-extension skip
- `force_include` can override oversize skip
- a non-forced manual include does not override those policy decisions

Out of scope here: architecture and layer boundaries, deployment and runtime operations, CLI command usage, Web UI walkthroughs, and an exhaustive rules reference beyond the accepted baseline.
