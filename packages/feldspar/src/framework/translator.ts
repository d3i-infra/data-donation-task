import { isTranslatable, Text, Translatable } from './types/elements'

export const MISSING_TRANSLATION = '?text?'

export const Translator = (function () {
  let defaultLocale: string = 'nl'

  function setDefaultLocale (locale: string): void {
    defaultLocale = locale
  }

  function translate (text: Text, locale: string): string {
    if (typeof text === 'string') {
      return text
    }
    if (isTranslatable(text)) {
      return resolve(text, locale)
    }
    throw new TypeError('Unknown text type')
  }

  function resolve (translatable: Translatable, locale: string): string {
    const translations = translatable?.translations ?? {}
    const text = translations[locale]
    if (typeof text === 'string') {
      return text
    }
    const defaultText = translations[defaultLocale]
    if (typeof defaultText === 'string') {
      return defaultText
    }
    const first = Object.values(translations).find((value) => typeof value === 'string')
    if (first !== undefined) {
      return first
    }
    return MISSING_TRANSLATION
  }

  return { translate, setDefaultLocale }
})()
