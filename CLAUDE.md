# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture decisions (adg)

Working agreements live as lean ADRs in `docs/decisions/`, compiled into a brief.
**Consult them while planning a change, not just at write time** — a PreToolUse hook
injects the brief on edits, but pull it yourself *before* you design the change:

    adg lean brief --model docs/decisions <paths you expect to touch>

Treat the brief as constraints:
- **Invariant** → a hard rule; read the full ADR before planning.
- **Forbidden scope matched** → stop and surface the conflict; don't build it.
- **Companions** → check whether the related files also need edits.
- **No brief appeared** → never assume no rule applies.

After editing, run `adg lean index --model docs/decisions --root .` and the tests. If the
change establishes a reusable pattern, record it with `adg lean new`.

> The brief hook and the `adg lean` calls above need `adg` on your PATH — install the
> prebuilt binary (no Go toolchain):
> `curl -fsSL https://raw.githubusercontent.com/daniellemccool/ad-guidance-tool/main/install.sh | sh`
> The copy bundled in the `write-adr` plugin serves only that plugin's authoring skills;
> it is not on PATH. No brief is no excuse.

## Project basics

pnpm monorepo (the Feldspar data-donation workflow). TypeScript/React packages under
`packages/` (`@eyra/feldspar`, `@eyra/data-collector`, built with Vite); Python Pyodide
runtime in `packages/python` (Poetry).

- Build: `pnpm build`
- All tests (JS + Python): `pnpm test` (Python only: `pnpm test:py`)
- JS unit tests: `pnpm --filter @eyra/feldspar test`
- Typecheck: `pnpm typecheck:py`
- E2E (Playwright): `pnpm test:e2e`
- Memory benchmarks: `scripts/benchmarks/` (see its README)
