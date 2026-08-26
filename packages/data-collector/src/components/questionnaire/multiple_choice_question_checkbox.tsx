import React from 'react'
import {
  ReactFactoryContext,
  Title3,
} from "@eyra/feldspar"
import { resolveText } from '../../locale/text'

import { PropsUIQuestionMultipleChoiceCheckbox } from './types'

interface parentSetter {
  parentSetter: (arg: any) => any
}

type Props = PropsUIQuestionMultipleChoiceCheckbox & parentSetter & ReactFactoryContext

export const MultipleChoiceQuestionCheckbox = (props: Props): React.JSX.Element => {
  const { question, choices, id, parentSetter, locale } = props
  const [selectedChoices, setSelectedChoices] = React.useState<string[]>([]);

  const copy = prepareCopy(locale)

  const setParentState = ()  => {
    parentSetter((prevState: any) => {
       prevState[id] = selectedChoices
       return prevState
    })
  }

  React.useEffect(() => {
      setParentState()
  })

  const handleChoiceSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const { value, checked } = event.target;
    if (checked) {
      setSelectedChoices((prevSelectedChoices) => [
        ...prevSelectedChoices,
        value,
      ]);
    } else {
      setSelectedChoices((prevSelectedChoices) =>
        prevSelectedChoices.filter((choice) => choice !== value)
      );
    }
  };

  return (
    <div className="p-4">
      <Title3 text={copy.question} />
      <ul className="mt-4 space-y-1">
        {copy.choices.map((choice, index) => (
          <li key={index}>
            <label className="flex items-center">
              <input
                type="checkbox"
                name="choice"
                value={choice}
                checked={selectedChoices.includes(choice)}
                onChange={handleChoiceSelect}
                className="mr-1 form-checkbox"
              />
              {choice}
            </label>
          </li>
        ))}
      </ul>
    </div>
  );

  function prepareCopy (locale: string): Copy {
    return {
      choices: choices.map((choice) => resolveText(choice, locale)),
      question: resolveText(question, locale)
    }
  }
}

interface Copy {
  choices: string[]
  question: string
}
