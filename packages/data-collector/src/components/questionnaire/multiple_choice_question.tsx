import React from 'react'
import {
  ReactFactoryContext,
  Title3,
} from "@eyra/feldspar"
import { resolveText } from '../../locale/text'
import { PropsUIQuestionMultipleChoice } from './types'

interface parentSetter {
  parentSetter: (arg: any) => any
}

type Props = PropsUIQuestionMultipleChoice & parentSetter & ReactFactoryContext

export const MultipleChoiceQuestion = (props: Props): React.JSX.Element => {
  const { question, choices, id, parentSetter, locale } = props
  const [selectedChoice, setSelectedChoice] = React.useState<string>("");
  const [checkedArray, setCheckedArray] = React.useState(Array(choices.length).fill(false));

  const copy = prepareCopy(locale)

  const handleChoiceSelect = (choice: string, index: number) => {
    setSelectedChoice(choice)
    setCheckedArray(Array.from({ length: choices.length }, (_, i) => i === index))
  };

  const setParentState = ()  => {
    parentSetter((prevState: any) => {
       prevState[id] = selectedChoice
       return prevState
    })
  }

  React.useEffect(() => {
      setParentState()
  })

  return (
    <div className="p-4">
      <Title3 text={copy.question}/>
      <ul className="mt-4 space-y-1">
        {copy.choices.map((choice, index) => (
          <li key={index}>
            <label className="inline-flex items-center">
              <input
                type="radio"
                name={`${index}-${id}`}
                value={choice}
                checked={checkedArray.at(index)}
                onChange={() => handleChoiceSelect(choice, index)}
                className="mr-1 form-radio"
              />
            </label>
            {choice}
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

