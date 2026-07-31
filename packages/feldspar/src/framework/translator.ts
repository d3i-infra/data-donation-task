import { isTranslatable, Text, Translatable } from './types/elements'

export const Translator = (function () {
  const defaultLocale: string = 'en'

  function translate (text: Text, locale: string): string {
    if (typeof text === 'string') {
      return text
    }

    if (isTranslatable(text)) {
      return resolve(text, locale)
    }

    throw new TypeError('Unknown text type')
  }

  function resolve (
    translatable: Translatable,
    locale: string
  ): string {
    const text = translatable.translations[locale]

    if (text !== null && text !== undefined) {
      return text
    }

    const defaultText =
      translatable.translations[defaultLocale]

    if (
      defaultText !== null &&
      defaultText !== undefined
    ) {
      return defaultText
    }

    const availableText = Object.values(
      translatable.translations
    ).find(
      (value): value is string =>
        value !== null && value !== undefined
    )

    return availableText ?? '?text?'
  }

  return {
    translate
  }
})()