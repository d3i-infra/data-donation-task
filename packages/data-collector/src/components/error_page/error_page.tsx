import { ReactElement } from "react"
import {
  Title1,
  BodyLarge,
  ReactFactoryContext,
} from "@eyra/feldspar"
import TextBundle from "@eyra/feldspar"
import { resolveText } from "../../locale/text"
import { PropsUIPageError } from "./types"

type Props = PropsUIPageError & ReactFactoryContext

export const ErrorPage = (props: Props): ReactElement => {

  const { message } = props
  const { title, text } = prepareCopy(props)

  return (
    <div>
      <Title1 text={title} />
      <BodyLarge text={text} />
      <BodyLarge text={message} />
    </div>
  )
}

interface Copy {
  title: string
  text: string
}

function prepareCopy ({ locale }: Props): Copy {
  return {
    title: resolveText(title, locale),
    text: resolveText(text, locale)
  }
}

const title = new TextBundle()
  .add('en', 'Error, not your fault!')
  .add('nl', 'Foutje, niet jouw schuld!')
  .add('de', 'Fehler, nicht Ihre Schuld!')
  .add('it', 'Errore, non è colpa sua!')
  .add('es', '¡Error, no es culpa suya!')

const text = new TextBundle()
  .add('en', 'Consult the researcher, or close the page')
  .add('nl', 'Raadpleeg de onderzoeker of sluit de pagina')
  .add('de', 'Wenden Sie sich an den Forscher oder schließen Sie die Seite')
  .add('it', 'Consulti il ricercatore oppure chiuda la pagina')
  .add('es', 'Consulte al investigador o cierre la página')
