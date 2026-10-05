import {
  DonateButtons,
  BodyLarge,
  ReactFactoryContext,
} from "@eyra/feldspar"
import TextBundle from "@eyra/feldspar"
import { resolveText } from "../../locale/text"
import {
    TableWithContext,
    PropsUIPromptConsentFormViz,
    PropsUITableRow,
    PropsUITableHead,
} from "./types"
import { useCallback, useEffect, useRef, useState, ReactElement } from "react"
import _ from "lodash"
import { TableContainer } from "./table_container"
import { parseTables } from "./parse_table"

type Props = PropsUIPromptConsentFormViz & ReactFactoryContext

export const ConsentFormViz = (props: Props): ReactElement => {
  const [tables, setTables] = useState<TableWithContext[]>(() => parseTables(props.tables, props.locale))
  const { locale, resolve } = props
  const { description } = prepareCopy(props)
  // The state initializer above already parsed props.tables; only re-parse
  // when the host actually sends new tables (issue #122 double parse).
  const parsedTables = useRef(props.tables)

  useEffect(() => {
    if (parsedTables.current === props.tables) return
    parsedTables.current = props.tables
    setTables(parseTables(props.tables, props.locale))
    // eslint-disable-next-line react-hooks/exhaustive-deps -- PENDING_ISSUES "lint hygiene" entry 2026-08-26: consent_form_viz re-parse effect intentionally omits `props.locale` from deps; the effect's ref-comparison guard (not this array) enforces ADR-0031's parse-once contract (issue #122 double parse) and only ever fires on a new `props.tables` identity.
  }, [props.tables])

  const updateTable = useCallback((tableId: string, table: TableWithContext) => {
    setTables((tables) => {
      const index = tables.findIndex((table) => table.id === tableId)
      if (index === -1) return tables

      const newTables = [...tables]
      newTables[index] = table
      return newTables
    })
  }, [])

  function handleDonate(): void {
    const value = serializeConsentData()
    resolve?.({ __type__: "PayloadJSON", "value": value })
  }

  function handleCancel(): void {
    resolve?.({ __type__: "PayloadFalse", value: false })
  }

  function serializeConsentData(): string {
    const array = serializeTables()
    return JSON.stringify(array)
  }

  function serializeTables(): any[] {
    return tables.map((table) => serializeTable(table))
  }


  function serializeTable({ id, head, body: { rows }, deletedRowCount }: TableWithContext): any {
    const data = rows.map((row) => serializeRow(row, head))
    return { [id]: data, "deleted row count": deletedRowCount.toString() }
  }

  function serializeRow(row: PropsUITableRow, head: PropsUITableHead): any {
    const keys = head.cells.map((cell) => cell)
    const values = row.cells.map((cell) => cell)
    return _.fromPairs(_.zip(keys, values))
  }

  return (
    <>
      <div className="max-w-3xl">
        {description.split("\n").map((line, index) => (
          <BodyLarge key={"description" + String(index)} text={line} />
        ))}
      </div>
      <div className="flex flex-col gap-16 w-full">
        <div className="grid gap-8 max-w-full">
          {tables.map((table) => {
            return (
              <TableContainer key={table.id} id={table.id} table={table} updateTable={updateTable} locale={locale} />
            )
          })}
        </div>
        <DonateButtons
          onDonate={handleDonate}
          onCancel={handleCancel}
          locale={locale}
          donateQuestion={props.donateQuestion ?? defaultDonateQuestionLabel}
          donateButton={props.donateButton ?? defaultDonateButtonLabel}
        />
      </div>
    </>
  )
}

interface Copy {
  description: string
}

function prepareCopy({ description, locale }: Props): Copy {
  return {
    description: resolveText(description ?? defaultDescription, locale),
  }
}

const defaultDonateQuestionLabel = new TextBundle()
  .add('en', 'Do you want to share the above data?')
  .add('de', 'Möchten Sie die oben genannten Daten teilen?')
  .add('nl', 'Wilt u de bovenstaande gegevens delen?')
  .add('it', 'Vuole condividere i dati sopra riportati?')
  .add('es', '¿Desea compartir los datos anteriores?')

const defaultDonateButtonLabel = new TextBundle()
  .add('en', 'Yes, share for research')
  .add('de', 'Ja, für Forschung teilen')
  .add('nl', 'Ja, deel voor onderzoek')
  .add('it', 'Sì, condividi per la ricerca')
  .add('es', 'Sí, compartir para la investigación')

const defaultDescription = new TextBundle()
  .add('en', 'Determine whether you would like to share the data below. Carefully check the data and adjust when required. With your contribution, you help the previously described research. Thank you in advance.')
  .add('de', 'Legen Sie fest, ob Sie die untenstehenden Daten teilen möchten. Überprüfen Sie die Daten sorgfältig und passen Sie sie bei Bedarf an. Mit Ihrem Beitrag helfen Sie der zuvor beschriebenen Forschung. Vielen Dank im Voraus.')
  .add('nl', 'Bepaal of u de onderstaande gegevens wilt delen. Bekijk de gegevens zorgvuldig en pas zo nodig aan. Met uw bijdrage helpt u het eerder beschreven onderzoek. Alvast hartelijk dank.')
  .add('it', 'Decida se desidera condividere i dati riportati di seguito. Controlli attentamente i dati e li modifichi se necessario. Con il suo contributo aiuta la ricerca descritta in precedenza. Grazie in anticipo.')
  .add('es', 'Decida si desea compartir los datos que aparecen a continuación. Revise los datos con atención y modifíquelos si es necesario. Con su contribución ayuda a la investigación descrita anteriormente. Muchas gracias de antemano.')

