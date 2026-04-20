# ZMTP Fuzz Corpus

Native Go fuzzing (`go test -fuzz`) corpus for the ZMTP parsers. Seed files
live in subdirectories named after the fuzz target:

- `FuzzFrameDecode/` — flags + length + body triples that the frame decoder
  must accept or reject cleanly (no panic, no runaway allocation).
- `FuzzGreeting/` — full 64-byte greeting blobs.
- `FuzzCommand/` — ZMTP command bodies (`READY`, `SUBSCRIBE`, `PING`, …).
- `FuzzMetadata/` — property-list payloads.
- `FuzzCurveEnvelope/` — CurveZMQ `MESSAGE` envelope bytes (populated in
  Wave 1 A6 once the CURVE codec lands).

Seeds should be raw binary files, one fixture per file. Add fixtures
captured from libzmq via the `interop/capture/` harness (Wave 1 A7) as
regressions are found.

Target functions live under `../../zmtp/` (Wave 1 A1). Until those land
this directory is a structural placeholder keeping the `testdata/fuzz/`
layout conventional so seed files can be dropped in as corpora are
captured.
