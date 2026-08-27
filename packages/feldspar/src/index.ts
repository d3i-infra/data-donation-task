export { ScriptHostComponent } from './components/script_host_component'
export type { ScriptHostProps } from './components/script_host_component'
export { Bridge } from './framework/types/modules'
export { default as FakeBridge } from './fake_bridge'
export { LiveBridge } from './live_bridge'
export { LogForwarder, WindowLogSource } from './framework/logging'
export type { LogEntry, LogLevel, Logger } from './framework/logging'
export {DataSubmissionPageFactory} from './framework/visualization/react/factories/data_submission_page'
export {PromptFactory} from './framework/visualization/react/ui/prompts/factory'
export {ReactFactoryContext} from './framework/visualization/react/factory'

// D3I additions to the upstream feldspar exports
export { default } from './framework/text_bundle'
export { Translator, MISSING_TRANSLATION } from './framework/translator'
export { Table } from './framework/types/commands'
export {
  Title1,
  Title2,
  Title3,
  Title4,
  BodyLarge,
  BodySmall,
} from './framework/visualization/react/ui/elements/text'
export {
  LabelButton,
  PrimaryButton,
} from './framework/visualization/react/ui/elements/button'
export {
  isInstanceOf,
} from './framework/helpers'
export { DonateButtons } from './framework/visualization/react/ui/prompts/donate_buttons'
