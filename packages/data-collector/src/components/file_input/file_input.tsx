import * as React from 'react'
import {
  Translatable,
  Translator,
  ReactFactoryContext,
  BodySmall,
  BodyLarge,
} from "@eyra/feldspar"
import { PrimaryButton } from "../ui/primary_button"
import TextBundle from "@eyra/feldspar"
import { PropsUIPromptFileInputD3I } from "./types"
import CloseSvg from "./assets/close.svg"

type Props = PropsUIPromptFileInputD3I & ReactFactoryContext

export const FileInput = (props: Props): JSX.Element => {
  if (props.multiple) {
    return <MultiFileInput {...props} />
  }
  return <SingleFileInput {...props} />
}

const SingleFileInput = (props: Props): JSX.Element => {
  const [waiting, setWaiting] = React.useState<boolean>(false)
  const [selectedFile, setSelectedFile] = React.useState<File>()
  const input = React.useRef<HTMLInputElement>(null)

  const { resolve } = props
  const description = Translator.translate(props.description, props.locale)

  function handleClick(): void {
    input.current?.click()
  }

  function handleSelect(event: React.ChangeEvent<HTMLInputElement>): void {
    const files = event.target.files
    if (files != null && files.length > 0) {
      setSelectedFile(files[0])
    } else {
      console.log('[FileInput] Error selecting file: ' + JSON.stringify(files))
    }
  }

  function handleConfirm(): void {
    if (selectedFile !== undefined && !waiting) {
      setWaiting(true)
      resolve?.({ __type__: 'PayloadFile', value: selectedFile })
    }
  }

  const placeholder = Translator.translate(placeholderLabel(), props.locale)
  const selectButton = Translator.translate(selectButtonLabel(), props.locale)
  const continueButton = Translator.translate(continueButtonLabel(), props.locale)
  const note = Translator.translate(noteLabel(), props.locale)

  return (
    <>
      <div id='select-panel'>
        <div className='flex-wrap text-bodylarge font-body text-grey1 text-left'>
          {description}
        </div>
        <div className='mt-8' />
        <div className='p-6 border-grey4 border-2 rounded'>
          <input ref={input} id='input' type='file' className='hidden' accept={props.extensions} onChange={handleSelect} />
          <div className='flex flex-col sm:flex-row gap-2 sm:gap-4 sm:items-center'>
            <BodyLarge text={selectedFile?.name ?? placeholder} margin='' color={selectedFile === undefined ? 'text-grey2' : 'text-grey1'} />
            <div className='grow' />
            <div className='flex-wrap'>
              <div className='flex flex-row'>
                <PrimaryButton onClick={handleClick} label={selectButton} color='bg-tertiary text-grey1' />
              </div>
            </div>
          </div>
        </div>
        <div className='mt-4' />
        <div className={`${selectedFile === undefined ? 'opacity-30' : 'opacity-100'}`}>
          <BodySmall text={note} margin='' />
          <div className='mt-8' />
          <div className='flex flex-row gap-4'>
            <PrimaryButton label={continueButton} onClick={handleConfirm} enabled={selectedFile !== undefined} spinning={waiting} />
          </div>
        </div>
      </div>
    </>
  )
}

const MultiFileInput = (props: Props): JSX.Element => {
  const [waiting, setWaiting] = React.useState<boolean>(false)
  const [files, setFiles] = React.useState<File[]>([])
  const input = React.useRef<HTMLInputElement>(null)

  const { resolve } = props
  const description = Translator.translate(props.description, props.locale)

  function handleClick(): void {
    input.current?.click()
  }

  function addFile(file: File): void {
    const fileExists = files.some(f => f.name === file.name && f.size === file.size)
    if (!fileExists) {
      setFiles(prevFiles => [...prevFiles, file])
    }
  }

  function removeFile(index: number): void {
    setFiles(prevFiles => prevFiles.filter((_, i) => i !== index))
  }

  function handleSelect(event: React.ChangeEvent<HTMLInputElement>): void {
    const selectedFiles = event.target.files
    if (selectedFiles != null && selectedFiles.length > 0) {
      for (let i = 0; i < selectedFiles.length; i++) {
        addFile(selectedFiles[i])
      }
    } else {
      console.log('[FileInput] Error selecting file: ' + JSON.stringify(selectedFiles))
    }
  }

  function handleConfirm(): void {
    if (files.length > 0 && !waiting) {
      setWaiting(true)
      resolve?.({ __type__: 'PayloadFileArray', value: files })
    }
  }

  const selectButton = Translator.translate(multiSelectButtonLabel(), props.locale)
  const continueButton = Translator.translate(continueButtonLabel(), props.locale)
  const note = Translator.translate(noteLabel(), props.locale)

  return (
    <>
      <div id='select-panel'>
        <div className='flex-wrap text-bodylarge font-body text-grey1 text-left'>
          {description}
        </div>
        <div className='mt-8' />
        <div className='p-6 border-grey4'>
          <input ref={input} id='input' type='file' className='hidden' accept={props.extensions} onChange={handleSelect} multiple />
          <div className='flex flex-row gap-4 items-center'>
            <PrimaryButton onClick={handleClick} label={selectButton} color='bg-tertiary text-grey1' />
          </div>
        </div>
        <div>
          {files.map((file, index) => (
            <div key={index} className="w-64 md:w-full px-4">
              <div className="flex items-center justify-between">
                <span className="truncate">{file.name}</span>
                <button onClick={() => removeFile(index)} className="shrink-0">
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

const continueButtonLabel = (): Translatable => {
  return new TextBundle()
    .add('en', 'Continue')
    .add('de', 'Weiter')
    .add('it', 'Continua')
    .add('es', 'Continuar')
    .add('nl', 'Verder')
    .add('ro', 'Continuați')
    .add('lt', 'Tęsti')
}

const multiSelectButtonLabel = (): Translatable => {
  return new TextBundle()
    .add('en', 'Choose file(s)')
    .add('de', 'Datei(en) auswählen')
    .add('nl', 'Kies bestand(en)')
}

const selectButtonLabel = (): Translatable => {
  return new TextBundle()
    .add('en', 'Choose file')
    .add('de', 'Datei auswählen')
    .add('it', 'Scegli file')
    .add('es', 'Elegir archivo')
    .add('nl', 'Kies bestand')
    .add('ro', 'Alegeți fișier')
    .add('lt', 'Pasirinkti failą')
}

const noteLabel = (): Translatable => {
  return new TextBundle()
    .add('en', 'Note: The process to extract the correct data from the file is done on your own device. No data is stored or sent yet.')
    .add('de', 'Hinweis: Der Prozess zur Extraktion der richtigen Daten aus der Datei erfolgt auf Ihrem eigenen Gerät. Es werden noch keine Daten gespeichert oder gesendet.')
    .add('it', 'Nota: Il processo per estrarre i dati corretti dal file viene eseguito sul Suo dispositivo. Nessun dato viene ancora memorizzato o inviato.')
    .add('es', 'Nota: El proceso para extraer los datos correctos del archivo se realiza en su propio dispositivo. Aún no se almacena ni se envía ningún dato.')
    .add('nl', 'Let op: Het proces om de juiste gegevens uit het bestand te halen wordt uitgevoerd op uw eigen apparaat. Er worden nog geen gegevens opgeslagen of verzonden.')
    .add('ro', 'Notă: Procesul de extragere a datelor corecte din fișier se realizează pe propriul dvs. dispozitiv. Nu sunt stocate sau trimise încă date.')
    .add('lt', 'Pastaba: Tinkamų duomenų išgavimas iš failo atliekamas jūsų įrenginyje. Duomenys dar nėra saugomi ar siunčiami.')
}

const placeholderLabel = (): Translatable => {
  return new TextBundle()
    .add('en', 'E.g. data.zip')
    .add('de', 'Z.B. data.zip')
    .add('it', 'Esempio: data.zip')
    .add('es', 'Ejemplo: data.zip')
    .add('nl', 'Voorbeeld: data.zip')
    .add('ro', 'Ex. data.zip')
    .add('lt', 'Pvz. data.zip')
}
