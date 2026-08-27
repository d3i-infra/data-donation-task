import React from 'react'
import {
  ReactFactoryContext,
  PrimaryButton,
} from "@eyra/feldspar"
import TextBundle from "@eyra/feldspar"
import { resolveText } from "../../locale/text"
import { 
    PropsUIPromptQuestionnaire,
    isPropsUIQuestionMultipleChoice,
    isPropsUIQuestionMultipleChoiceCheckbox,
    isPropsUIQuestionOpen,
} from "./types"

import { MultipleChoiceQuestion } from './multiple_choice_question'
import { MultipleChoiceQuestionCheckbox } from './multiple_choice_question_checkbox'
import { OpenQuestion } from './open_question'

type Props = PropsUIPromptQuestionnaire & ReactFactoryContext

export const Questionnaire = (props: Props): React.JSX.Element => {
  const { questions, description, resolve, locale } = props
  const [answers, setAnswers] = React.useState<{}>({});
  const copy = prepareCopy(locale)

  function handleDonate (): void {
    const value = JSON.stringify(answers)
    resolve?.({ __type__: 'PayloadJSON', value })
  }

  const renderQuestion = (item: any) => {
    if (isPropsUIQuestionMultipleChoice(item)) {
      return (
        <div key={item.id}>
          <MultipleChoiceQuestion {...item} locale={locale} parentSetter={setAnswers} />
        </div>
      )
    }
    if (isPropsUIQuestionMultipleChoiceCheckbox(item)) {
      return (
        <div key={item.id}>
          <MultipleChoiceQuestionCheckbox {...item} locale={locale} parentSetter={setAnswers} />
        </div>
      )
    }
    if (isPropsUIQuestionOpen(item)) {
      return (
        <div key={item.id}>
          <OpenQuestion {...item} locale={locale} parentSetter={setAnswers} />
        </div>
      )
    }
  }

  const renderQuestions = () => {
   return questions.map((item) => renderQuestion(item))
  }

  return (
    <div>
      <div className='flex-wrap text-bodylarge font-body text-grey1 text-left'>
        {copy.description}
      </div>
      <div>
        {renderQuestions()}
      </div>
      <div className='flex flex-row gap-4 mt-4 mb-4'>
        <PrimaryButton label={copy.continueLabel} onClick={handleDonate} color='bg-success text-white' />
      </div>
    </div>
  );

        
  function prepareCopy (locale: string): Copy {
    return {
      description: resolveText(description, locale),
      continueLabel: resolveText(continueLabel, locale)
    }
  }
};


interface Copy {
  description: string
  continueLabel: string
}

const continueLabel = new TextBundle()
  .add('en', 'Continue')
  .add('nl', 'Doorgaan')
  .add('de', 'Weiter')
  .add('it', 'Continua')
  .add('es', 'Continuar')
 
