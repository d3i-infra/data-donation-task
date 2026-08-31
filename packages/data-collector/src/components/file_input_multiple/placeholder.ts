import TextBundle from "@eyra/feldspar"
import { resolveText } from "../../locale/text"
import { Translatable } from "./types"

// Split out of file_input_multiple.tsx so it can be unit-tested without
// pulling in React/JSX or the component's asset (SVG) imports — this
// package's jest config (testMatch: ['**/*.test.ts']) has no transform for
// those, so a test importing the .tsx directly fails to parse.

export const placeholderText = (): Translatable => {
  return new TextBundle()
    .add('en', 'E.g. data.zip')
    .add('de', 'Z.B. data.zip')
    .add('nl', 'Voorbeeld: data.zip')
    .add('it', 'Esempio: data.zip')
    .add('es', 'Ejemplo: data.zip')
}

/**
 * Prefers the protocol-supplied example (props.example, from d3i_props.py's
 * optional `example` field) when present; falls back to the component's own
 * built-in per-locale placeholder copy when absent. No current caller of
 * `port_helpers.render_file_page(multiple=True)` (including e2etest_multifile)
 * actually omits the field — it is always set for a multi-file prompt — so
 * this fallback is unit-covered defense against an absent `example`, not a
 * behavior any live platform exercises today.
 */
export function resolvePlaceholder (example: Translatable | undefined, locale: string): string {
  return example !== undefined ? resolveText(example, locale) : resolveText(placeholderText(), locale)
}
