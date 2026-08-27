import * as React from 'react'
import {
  ReactFactoryContext,
  PrimaryButton,
  BodySmall
} from "@eyra/feldspar"
import TextBundle from "@eyra/feldspar"
import { resolveText } from "../../locale/text"
import { PropsUIPromptFileInputMultiple, Translatable } from "./types.ts"
import CloseSvg  from "./assets/close.svg"

type Props = PropsUIPromptFileInputMultiple & ReactFactoryContext

// The type `resolve` actually accepts (feldspar's `Payload` union, not exported
// from @eyra/feldspar's public API — ADR-0002 forbids adding the export just for
// this cast). Derived structurally so the bridge cast below stays honest without
// duplicating feldspar's Payload union here.
type ResolvePayload = Parameters<NonNullable<Props['resolve']>>[0]

export const FileInputMultiple = (props: Props): React.JSX.Element => {
  const [waiting, setWaiting] = React.useState<boolean>(false)
  const [files, setFiles] = React.useState<File[]>([])
  const input = React.useRef<HTMLInputElement>(null)

  const { resolve } = props
  const { description, note, extensions, selectButton, continueButton } = prepareCopy(props)

  function handleClick (): void {
    input.current?.click()
  }

  function addFile(file: File): void {
    const fileExists = files.some(f => f.name === file.name && f.size === file.size);
    if (!fileExists) {
      setFiles(prevFiles => [...prevFiles, file]);
    }
  };

  function removeFile(index: number): void {
    setFiles(prevFiles => prevFiles.filter((_, i) => i !== index));
  };

  function handleSelect (event: React.ChangeEvent<HTMLInputElement>): void {
    const selectedFiles = event.target.files
    if (selectedFiles != null && selectedFiles.length > 0) {
			for (let i = 0; i < selectedFiles.length; i++) {
				addFile(selectedFiles[i])
			}
    } else {
      console.log('[FileInput] Error selecting file: ' + JSON.stringify(selectedFiles))
    }
  }

  function handleConfirm (): void {
    if (files !== undefined && !waiting) {
      setWaiting(true)
      // `PayloadFileArray` is not (and was never) a member of feldspar's Payload
      // union: packages/python/port/helpers/flow_builder.py only recognizes
      // `__type__ === "PayloadFile"` and silently skips anything else, so this
      // call is already inert at runtime. Bridge cast lets tsc reflect that
      // honestly instead of masking it; Task 5 rebuilds this component's donate
      // path and removes the cast then.
      resolve?.({ __type__: 'PayloadFileArray', value: files } as unknown as ResolvePayload)
    }
  }

  return (
    <>
      <div id='select-panel'>
        <div className='flex-wrap text-bodylarge font-body text-grey1 text-left'>
          {description}
        </div>
        <div className='mt-8' />
        <div className='p-6 border-grey4 '>
          <input ref={input} id='input' type='file' className='hidden' accept={extensions} onChange={handleSelect} multiple/>
          <div className='flex flex-row gap-4 items-center'>
            <PrimaryButton onClick={handleClick} label={selectButton} color='bg-tertiary text-grey1' />
          </div>
        </div>
        <div>
        {files.map((file, index) => (
            <div className="w-64 md:w-full px-4">
                <div key={index} className="flex items-center justify-between">
                    <span className="truncate">{file.name}</span>
                    <button
                        onClick={() => removeFile(index)}
                        className="shrink-0"
                    >
                    <img src={CloseSvg} className={"w-8 h-8"} />
                    </button>
                </div>
                <div className="w-full mt-2">
                    <hr className="border-grey4" />
                </div>
            </div>
        ))}
        </div>
        <div className='mt-4' />
        <div className={`${files[0] === undefined ? 'opacity-30' : 'opacity-100'}`}>
          <BodySmall text={note} margin='' />
          <div className='mt-8' />
          <div className='flex flex-row gap-4'>
            <PrimaryButton label={continueButton} onClick={handleConfirm} enabled={files[0] !== undefined} spinning={waiting} />
          </div>
        </div>
      </div>
    </>
  )
}

interface Copy {
  description: string
  note: string
  extensions: string
  selectButton: string
  continueButton: string
}

function prepareCopy ({ description, extensions, locale }: Props): Copy {
  return {
    description: resolveText(description, locale),
    note: resolveText(note(), locale),
    extensions: extensions,
    selectButton: resolveText(selectButtonLabel(), locale),
    continueButton: resolveText(continueButtonLabel(), locale)
  }
}

const continueButtonLabel = (): Translatable => {
  return new TextBundle()
    .add('en', 'Continue')
    .add('de', 'Weiter')
    .add('nl', 'Verder')
    .add('it', 'Continua')
    .add('es', 'Continuar')
}

const selectButtonLabel = (): Translatable => {
  return new TextBundle()
    .add('en', 'Choose file(s)')
    .add('de', 'Datei(en) auswählen')
    .add('nl', 'Kies bestand(en)')
    .add('it', 'Scegli file')
    .add('es', 'Elegir archivo(s)')
}

const note = (): Translatable => {
  return new TextBundle()
    .add('en', 'Note: The process to extract the correct data from the file is done on your own computer. No data is stored or sent yet.')
    .add('de', 'Anmerkung: Die weitere Verarbeitung der Datei erfolgt auf Ihrem eigenen Endgerät. Es werden noch keine Daten gespeichert oder weiter gesendet.')
    .add('nl', 'NB: Het proces om de juiste gegevens uit het bestand te halen gebeurt op uw eigen computer. Er worden nog geen gegevens opgeslagen of verstuurd.')
    .add('it', "Nota: l'estrazione dei dati corretti dal file avviene sul suo computer. Non viene ancora salvato né inviato alcun dato.")
    .add('es', 'Nota: el proceso de extraer los datos correctos del archivo se realiza en su propio ordenador. Todavía no se guarda ni se envía ningún dato.')
}

