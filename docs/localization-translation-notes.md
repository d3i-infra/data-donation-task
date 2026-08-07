# Localization translation notes (de, it, es)

The German, Italian, and Spanish UI strings that ship with this repo are
**provisional**: machine-translated, then read through once by a non-native
speaker for register and consistency. They have *not* been reviewed by a native
speaker. This page is what a reviewer needs in order to do that review — the
conventions the drafts follow, and the specific calls that were made under
uncertainty and should be confirmed or overruled.

English and Dutch are not provisional and are not covered here.

## Register conventions

The participant is a research subject being asked to hand over personal data, so
prose addresses them **formally** throughout:

| Language | Prose register | Example |
|---|---|---|
| German | `Sie` | *Möchten Sie diese Daten für die Forschung teilen?* |
| Italian | `Lei` (third-person formal) | *Vuole condividere questi dati per la ricerca?* |
| Spanish | `usted` | *¿Desea compartir estos datos para la investigación?* |

**Italian buttons deliberately break this.** Button labels use the `tu`
imperative — `Continua`, `Riprova`, `Sì, condividi per la ricerca` — rather than
the `Lei` forms (`Continui`, `Riprovi`, `Sì, condivida`). This is the convention
in Italian software UI: the label is read as the action the participant is
taking, not as an instruction the researcher issues. German and Spanish do not
have the same split, so their buttons stay consistent with their prose
(`Weiter`, `Continuar`).

If a native reviewer wants the Italian buttons in `Lei`, that is a coherent
alternative — but it has to be applied to *every* button, not one at a time.

## Flagged judgment calls

Each of these is a place where the draft picked one of several defensible
options. A reviewer should confirm or overrule; nothing here is a known error.

- **es — "selección cuidada"** (`helpers/flow_builder.py`): *"A continuación
  encontrará una selección cuidada de los datos de …"* renders the English
  "carefully selected" / Dutch "zorgvuldige selectie". "Cuidada" reads slightly
  literary; "una selección de los datos" or "los datos seleccionados" are
  plainer. Kept because the sentence is doing reassurance work — the participant
  is being told the extraction was deliberate, not exhaustive.
- **it — "Alimentari" → "Prodotti alimentari"** (`helpers/port_helpers.py`, the
  example questionnaire category): "Alimentari" alone reads as *grocery shops*
  in many contexts; "Prodotti alimentari" is unambiguously *food items*.
  Suggested change, not applied — it is example-flow text, and changing it
  changes a string researchers copy from.
- **es — "ordenador" vs "equipo"**
  (`components/file_input_multiple/file_input_multiple.tsx`): *"…se realiza en
  su propio ordenador"*. "Ordenador" is peninsular Spanish; Latin-American
  audiences say "computadora", and "equipo" (device) is the neutral option that
  also covers phones and tablets — which matters here, because participants do
  donate from phones. **This is an audience question, not a language question:**
  it depends on where the study recruits. Flagged rather than decided.
- **it — "sua" vs "Sua"** (`helpers/port_helpers.py`): the drafts use lowercase
  `sua` (*"Come valuterebbe la sua esperienza complessiva?"*). Capitalized `Sua`
  is the traditional courtesy form in formal Italian correspondence; lowercase is
  now normal in software and on the web. Kept lowercase for consistency with the
  button register above, but a reviewer may prefer `Sua` throughout — again,
  all-or-nothing.
- **de — "für die Forschung" vs "für Forschung"**
  (`helpers/port_helpers.py`): the consent *question* says *"…diese Daten für
  die Forschung teilen?"* (with article) while the *button* says *"Ja, für
  Forschung teilen"* (without). The button drops the article for length. This is
  the one flagged item that is arguably an inconsistency rather than a choice —
  a reviewer should decide whether the button can carry *"Ja, für die Forschung
  teilen"* without wrapping.

## Where the strings live

- `packages/python/port/helpers/port_helpers.py` — consent form, questionnaire,
  standard buttons.
- `packages/python/port/helpers/flow_builder.py` — per-platform flow prose.
- `packages/python/port/main.py` — the error/consent-gated error page.
- `packages/data-collector/src/components/**` — UI chrome (file input, retry
  prompt, error page, questionnaire widgets).
- Per-platform table titles, column headers, and visualization titles are **not**
  here — they live in `configs/<platform>_config.json`, generated from platform
  docstrings, and are the researcher's to translate. `en` is the only locale
  those must carry; see the README's localization matrix.

## Regenerating the review diff

The provisional translations landed in two commits. Their hashes are
branch-local and will not survive a rebase or squash, so find them by subject
and path rather than by hash. To see exactly what was added, in isolation from
the surrounding locale plumbing:

```sh
git log -p --grep="feat(py): translations introduced in the python bits" \
  --grep="feat: translations added to data-collector" -- \
  packages/python/port/helpers/port_helpers.py \
  packages/python/port/helpers/flow_builder.py \
  packages/python/port/main.py \
  packages/data-collector/src/components/
```

(at time of writing: `c04fb3d`, `25154f0`)

- `feat(py): translations introduced in the python bits`
- `feat: translations added to data-collector`

Reviewing that diff is reviewing the whole provisional set. When a native
speaker has signed off on a language, drop it from the `provisional` list in
`packages/data-collector/src/locale/ui_locales.json`, mirror the change into
`packages/python/port/helpers/ui_locales.json`, and update the README matrix.
