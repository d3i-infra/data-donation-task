# The data donation task

The data donation task (a fork of [Feldspar](https://github.com/eyra/feldspar)) is a front end that guides participants through the data donation steps, used in conjunction with Next.
Next is a software as a service platform developed by [Eyra](https://eyra.co/) to facilitate scientific research.

For detailed tutorials and API reference, see the [documentation site](https://d3i-infra.github.io/data-donation-task/).

### What's new in v3.0.0

Platform extraction is now config-driven: each platform has a `configs/<platform>_config.json` that declares table titles, column headers, and visualizations. Generate one with `pnpm generate-config <platform>`. The generator refuses to overwrite existing files, protecting researcher edits. `release.sh` auto-discovers platforms from `configs/` — no hardcoded list needed. `VITE_PLATFORM` is now required in dev mode.

See [CHANGELOG.md](CHANGELOG.md) for the full list of changes and the migration notes for downstream forks.

## Installation and local testing

### Pre-requisites

- Fork or clone this repo
- Install [Node.js](https://nodejs.org/en)
- Install [pnpm](https://pnpm.io/)
- Install [Python](https://www.python.org/)
- Install [Poetry](https://python-poetry.org/)

### Setup

```sh
pnpm install
cd packages/python && poetry install
```

### Check environment

```sh
pnpm doctor
```

### Start local dev server

```sh
VITE_PLATFORM=example pnpm start
```

Visit [`http://localhost:3000`](http://localhost:3000).

## Commands

### Development

| Command | Description |
|---|---|
| `VITE_PLATFORM=<platform> pnpm start` | Start dev server with hot reload |
| `pnpm generate-config <platform>` | Generate `configs/<platform>_config.json` from extractor docstrings |
| `pnpm run build` | Full production build (Python wheel + feldspar + data-collector) |
| `pnpm doctor` | Check environment setup (13 checks) |

### Testing & Type Checking

| Command | Description |
|---|---|
| `pnpm test` | Run the full suite (JS unit tests, then Python) |
| `pnpm test:js` | Run the JS unit tests (`@eyra/feldspar` + `@eyra/data-collector`) |
| `pnpm test:py` | Run the Python tests |
| `pnpm test:py -- tests/test_specific.py -q` | Run specific Python tests |
| `pnpm typecheck:py` | Run Pyright type checker |
| `pnpm verify:py` | Run both tests + type checks |
| `pnpm test:e2e` | Run the Playwright end-to-end suite |

### Releases

| Command | Description |
|---|---|
| `pnpm release` | Build one zip per platform (auto-discovered from `configs/`) |
| `VITE_PLATFORM=<platform> pnpm release` | Build a release zip for a single platform |

Releases are created in `releases/`.

## Working with platforms

Generate a config for a platform, then edit it to suit your study:

```sh
pnpm generate-config instagram
# edit packages/python/port/configs/instagram_config.json
```

To add a new platform, copy `packages/python/port/platforms/example.py` as your starting point.

See the [documentation site](https://d3i-infra.github.io/data-donation-task/) for full tutorials.

## Localization status

The participant-facing UI is localized in two independent layers. They are
translated by different people, cover different locales, and fall back
separately — a page can render German chrome around English table titles.

| UI locale | **Framework chrome**<br>buttons, consent prose, error pages<br>*(ships in this repo)* | **Study content**<br>table titles, column headers, viz titles<br>*(per platform, yours to write)* |
|---|---|---|
| `en` | complete — **the fallback** | **required**; validation fails without it |
| `nl` | complete | shipped for the platforms in this repo |
| `de` | ⚠️ **provisional** — machine-translated, pending native-speaker review | — falls back to English, live |
| `it` | ⚠️ **provisional** — machine-translated, pending native-speaker review | — falls back to English, live |
| `es` | ⚠️ **provisional** — machine-translated, pending native-speaker review | — falls back to English, live |

**Fallback is per string, at render time.** A locale that a given text bundle
does not carry resolves to `en` for that string only; there is no
whole-page language switch and no build step involved. So a `de` participant
sees translated chrome and English table titles until you translate the tables
in `configs/<platform>_config.json`.

Reviewing the provisional translations — register conventions and the specific
judgment calls that need a native speaker's ruling — is documented in
[`docs/localization-translation-notes.md`](docs/localization-translation-notes.md).

Checking your own coverage: `pnpm generate-config <platform>` and `pnpm release`
both run the config validator with `--report`, which prints a per-locale
coverage matrix (provisional locales marked `*`) and fails the build if a text
bundle is missing English.

### UI locale is not `platform_info.languages`

These are two unrelated things and are **never synced**:

- The **UI locale** (`en`/`nl`/`de`/`it`/`es`) is what language the interface
  renders in. It comes from the host at session start.
- **`platform_info.languages`** in a platform config — and `Language` in
  `port/helpers/validate.py` — describe the language of the *participant's DDP
  export*, i.e. what language the filenames and headers inside their downloaded
  zip are in. It is a parsing concern.

A participant can perfectly well read the UI in Spanish while donating a
Dutch-language Instagram export. Changing one must never change the other.

### Setting the locale in development

In production the locale comes only from the host's `live-init` message; there
is no URL override. In **dev builds only**, a `?locale=` query parameter
overrides it — this is the dev-server convenience and the Playwright e2e
injection point:

```
http://localhost:3000/?locale=nl
```

The value is normalized once, at the host boundary, before anything sees it:

| Requested | Rendered | Why |
|---|---|---|
| `nl` | `nl` | supported |
| `es-ES`, `es_ES`, `ES` | `es` | region and case are stripped |
| `ro` | `en` | not a supported UI locale → default |
| *(absent)* | `en` | default |

The supported set lives in
`packages/data-collector/src/locale/ui_locales.json` (mirrored into the Python
package; a test fails if the two drift). See ADR-0038.

## Architecture

See `docs/decisions/` for architectural decision records. Key structure:

```
packages/
  python/         Python extraction scripts (per-platform)
  feldspar/       Workflow UI framework (upstream Eyra)
  data-collector/ Host app / dev server with custom UI components
```

### Platform extraction flow

Each platform (Instagram, Facebook, YouTube, etc.) has a `FlowBuilder` subclass in `packages/python/port/platforms/` that handles:

1. File prompt → participant uploads DDP zip
2. Validation → DDP category detection via `DDP_CATEGORIES`
3. Extraction → `ZipArchiveReader` reads files from cached archive inventory
4. Consent → participant reviews extracted tables
5. Donation → data sent to host platform

### Supported platforms

LinkedIn, Instagram, Facebook, YouTube, TikTok, Netflix, ChatGPT, WhatsApp, X, Chrome

## Citation

If you use this repository in your research, please cite it as follows:

```
@article{Boeschoten2023,
  doi = {10.21105/joss.05596},
  url = {https://doi.org/10.21105/joss.05596},
  year = {2023},
  publisher = {The Open Journal},
  volume = {8},
  number = {90},
  pages = {5596},
  author = {Laura Boeschoten and Niek C. de Schipper and Adriënne M. Mendrik and Emiel van der Veen and Bella Struminskaya and Heleen Janssen and Theo Araujo},
  title = {Port: A software tool for digital data donation},
  journal = {Journal of Open Source Software}
}
```

You can find the full citation details in the [`CITATION.cff`](CITATION.cff) file.
