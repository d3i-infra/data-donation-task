// Matching feldspar's Text/Translatable (duplicated like consent_form_viz/types.ts
// to keep this component self-contained). Neither `Translatable` nor `Text` is part
// of @eyra/feldspar's public export surface (see packages/feldspar/src/index.ts), so
// they can't be imported — they're reproduced here instead. Previously `Text` was
// unimported and silently resolved to the DOM global Text node type.
export interface Translatable {
  translations: { [locale: string]: string }
}
export type Text = Translatable | string

export interface PropsUIPromptFileInputMultiple {
  __type__: "PropsUIPromptFileInputMultiple"
  // Always an object: PropsUIPromptFileInputMultiple.description on the Python side
  // (packages/python/port/api/d3i_props.py) is typed `props.Translatable` and
  // toDict() always calls `.description.toDict()` — the wire payload is always
  // `{translations: {...}}`, never a bare string, so this is `Translatable`, not
  // the wider `Text` (feldspar's own PropsUIPromptFileInput.description is `Text`,
  // but that type also isn't exported, so it isn't available to import here either).
  description: Translatable
  extensions: string
}
