import _ from 'lodash'
import { Translatable } from './types/elements'
import { MISSING_TRANSLATION } from './translator'

export default class TextBundle implements Translatable {
  translations: { [key: string]: string } = {}
  defaultLocale: string = 'nl'

  add (locale: string, text: string): TextBundle {
    this.translations[locale] = text
    return this
  }

  translate (locale: string): string {
    return _.escape(this.resolve(locale))
  }

  resolve (locale: string): string {
    const text = this.translations[locale]
    if (typeof text === 'string') {
      return text
    }

    const defaultText = this.translations[this.defaultLocale]
    if (typeof defaultText === 'string') {
      return defaultText
    }

    const first = Object.values(this.translations).find((value) => typeof value === 'string')
    if (first !== undefined) {
      return first
    }

    return MISSING_TRANSLATION
  }
}
