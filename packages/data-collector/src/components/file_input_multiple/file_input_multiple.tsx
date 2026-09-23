import * as React from 'react'
import {
  ReactFactoryContext,
  PrimaryButton,
  BodyLarge,
  BodySmall
} from "@eyra/feldspar"
import TextBundle from "@eyra/feldspar"
import { resolveText } from "../../locale/text"
import { PropsUIPromptFileInputMultiple, Translatable } from "./types.ts"
import { addFiles } from "./select"
import { resolvePlaceholder } from "./placeholder"
import CloseSvg  from "./assets/close.svg"

type Props = PropsUIPromptFileInputMultiple & ReactFactoryContext

export const FileInputMultiple = (props: Props): React.JSX.Element => {
  const [waiting, setWaiting] = React.useState<boolean>(false)
  const [files, setFiles] = React.useState<File[]>([])
  const [duplicates, setDuplicates] = React.useState<string[]>([])
  const input = React.useRef<HTMLInputElement>(null)

  const { resolve } = props
  const { description, note, placeholder, duplicatesNotice, extensions, selectButton, continueButton } = prepareCopy(props)

  function handleClick (): void {
    input.current?.click()
  }

  function removeFile(index: number): void {
    setDuplicates([]);
    setFiles(prevFiles => prevFiles.filter((_, i) => i !== index));
  };

  function handleSelect (event: React.ChangeEvent<HTMLInputElement>): void {
    const selectedFiles = event.target.files
    if (selectedFiles != null && selectedFiles.length > 0) {
      const { files: merged, duplicates: dupes } = addFiles(files, Array.from(selectedFiles))
      setFiles(merged)
      setDuplicates(dupes)
    } else {
      console.log('[FileInput] Error selecting file: ' + JSON.stringify(selectedFiles))
    }
    // Reset the native input's value: without this, re-picking the exact
    // same file(s) leaves the input's value unchanged, so the browser never
    // fires another `change` event and handleSelect never runs again — the
    // most common duplicate-selection path (re-opening the picker and
    // choosing the same file) would silently do nothing instead of
    // triggering addFiles' duplicate-notice path above.
    event.target.value = ""
  }

  function handleConfirm (): void {
    if (files !== undefined && !waiting) {
      setWaiting(true)
      resolve?.({ __type__: 'PayloadFiles', value: files })
    }
  }

  return (
    <>
      <div id='select-panel'>
        <div className='flex-wrap text-bodylarge font-body text-grey1 text-left'>
          {description}
        </div>
        <div className='mt-8' />
        <div className='p-6 border-grey4 border-2 rounded'>
          <input ref={input} id='input' type='file' className='hidden' accept={extensions} onChange={handleSelect} multiple/>
          <div className='flex flex-col sm:flex-row gap-2 sm:gap-4 items-center'>
            {files.length === 0 && (
              <BodyLarge text={placeholder} margin='' color='text-grey2' />
            )}
            <div className='grow' />
            <div className='flex-wrap'>
              <div className='flex flex-row'>
                <PrimaryButton onClick={handleClick} label={selectButton} color='bg-tertiary text-grey1' />
              </div>
            </div>
          </div>
        </div>
        {duplicates.length > 0 && (
          <>
            <div className='mt-2' />
            <BodySmall text={duplicatesNotice.replace('{names}', duplicates.join(', '))} margin='' />
          </>
        )}
        <div>
        {files.map((file, index) => (
            <div key={`${file.name} ${file.size} ${file.lastModified}`} className="w-64 md:w-full px-4">
                <div className="flex items-center justify-between">
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
  placeholder: string
  duplicatesNotice: string
  extensions: string
  selectButton: string
  continueButton: string
}

function prepareCopy ({ description, extensions, example, locale }: Props): Copy {
  return {
    description: resolveText(description, locale),
    note: resolveText(note(), locale),
    placeholder: resolvePlaceholder(example, locale),
    duplicatesNotice: resolveText(duplicatesNoticeText(), locale),
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

const duplicatesNoticeText = (): Translatable => {
  return new TextBundle()
    .add('en', 'Already added: {names}')
    .add('de', 'Bereits hinzugefügt: {names}')
    .add('nl', 'Al toegevoegd: {names}')
    .add('it', 'Già aggiunto: {names}')
    .add('es', 'Ya añadido: {names}')
}
