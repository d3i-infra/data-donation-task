// Matching feldspar's Text/Translatable (duplicated like consent_form_viz/types.ts
// to keep this component self-contained — feldspar doesn't export these types).
// Previously `Text` was unimported and silently resolved to the DOM global Text
// node type.
export interface Translatable {
  translations: { [locale: string]: string }
}
export type Text = Translatable | string

export interface PropsUIPromptFileInputMultiple {
  __type__: "PropsUIPromptFileInputMultiple"
  description: Text
  extensions: string
}
