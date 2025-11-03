import {
  BodyLarge,
  Translator,
  ReactFactoryContext,
  PrimaryButton,
} from "@eyra/feldspar"
import { PropsUIPromptRetry } from "./types"

type Props = PropsUIPromptRetry & ReactFactoryContext

import { JSX } from 'react'

export const RetryPrompt = (props: Props): JSX.Element => {
  const { resolve } = props
  const { text, ok } = prepareCopy(props)

  function handleOk (): void {
    resolve?.({ __type__: 'PayloadTrue', value: true })
  }

  return (
    <>
      <BodyLarge text={text} margin='mb-4' />
      <div className='flex flex-row gap-4'>
        <PrimaryButton label={ok} onClick={handleOk} color='text-grey1 bg-tertiary' />
      </div>
    </>
  )
}

interface Copy {
  text: string
  ok: string
}

function prepareCopy ({ text, ok, locale }: Props): Copy {
  return {
    text: Translator.translate(text, locale),
    ok: Translator.translate(ok, locale),
  }
}
