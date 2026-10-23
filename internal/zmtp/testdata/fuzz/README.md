# ZMTP Fuzz Corpus

Native Go fuzzing (`go test -fuzz`) corpus for the ZMTP parsers. Seed files
live in subdirectories named after the fuzz target:

- `FuzzFrameDecode/` — flags + length + body triples that the frame decoder
  must accept or reject cleanly (no panic, no runaway allocation).
- `FuzzCommand/` — ZMTP command bodies (`READY`, `SUBSCRIBE`, `PING`, …).
- `FuzzMetadata/` — property-list payloads.

Seeds should be raw binary files, one fixture per file. Add fixtures
captured from libzmq via the interop harness as regressions are found.

Target functions live under `../..`.
