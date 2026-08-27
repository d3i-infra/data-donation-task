// Matching feldspar's Text/Translatable (duplicated like consent_form_viz/types.ts
// to keep this component self-contained — feldspar doesn't export these types).
export interface Translatable {
  translations: { [locale: string]: string }
}
export type Text = Translatable | string

export interface PropsUIPromptRetry {
  __type__: 'PropsUIPromptRetry'
  text: Text
  ok: Text
}
