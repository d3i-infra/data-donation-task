import { useMemo, ReactElement } from 'react'
import TextBundle from '@eyra/feldspar'
import { resolveAll } from '../../locale/text'
import { TableWithContext } from './types'
import UndoSvg from './assets/images/undo.svg'

interface Props {
  table: TableWithContext
  searchedTable: TableWithContext
  handleUndo: () => void
  locale: string
}

export const TableItems = ({ table, searchedTable, handleUndo, locale }: Props): ReactElement => {
  const text = useMemo(() => getTranslations(locale), [locale])

  const deleted = table.deletedRowCount
  const n = table.body.rows.length
  const searched = searchedTable.body.rows.length
  const total = table.originalBody.rows.length - table.deletedRowCount

  const nLabel = n.toLocaleString(locale, { useGrouping: true })
  const totalLabel = total.toLocaleString(locale, { useGrouping: true })
  const searchLabel = searched.toLocaleString(locale, { useGrouping: true })

  const deletedLabel =
    deleted.toLocaleString(locale, { useGrouping: true }) +
    ' ' +
    text.deleted

  function entriesLabel(): string {
    if (n === 0) return text.noData

    // When a search is active, show:
    // "3 / 20 Einträge"
    if (searched < n) {
      return (
        searchLabel +
        ' / ' +
        nLabel +
        ' ' +
        (n === 1 ? text.entry : text.entries)
      )
    }

    // Singular / plural
    return (
      nLabel +
      ' ' +
      (n === 1 ? text.entry : text.entries)
    )
  }

  return (
    <div className='flex min-w-[200px] gap-1'>
      <div
        key={`${totalLabel}_${deleted}`}
        className='flex flex-wrap items-center gap-x-2 animate-fadeIn text-title7 md:text-title6 font-label'
      >
        <div key={totalLabel} className='animate-fadeIn'>
          {entriesLabel()}
          {deleted > 0 ? ',' : ''}
        </div>

        <div className={`flex text-grey2 ${deleted > 0 ? '' : 'hidden'}`}>
          {deletedLabel}
          <img
            src={UndoSvg}
            className='w-5 h-5 -translate-y-[2px] md:-translate-y-0 -translate-x-[3px] ml-2'
            onClick={handleUndo}
          />
        </div>
      </div>
    </div>
  )
}

function getTranslations (locale: string): Record<string, string> {
  return resolveAll(translations, locale)
}

const translations = {
  entry: new TextBundle()
    .add('en', 'entry')
    .add('nl', 'item')
    .add('de', 'Eintrag')
    .add('pl', 'wpis')
    .add('tr', 'kayıt')
    .add('ar', 'إدخال')
    .add('ru', 'запись')
    .add('it', 'voce')
    .add('ro', 'înregistrare')
    .add('es', 'entrada')
    .add('sq', 'hyrje'),

  entries: new TextBundle()
    .add('en', 'entries')
    .add('nl', 'items')
    .add('de', 'Einträge')
    .add('pl', 'wpisy')
    .add('tr', 'kayıt')
    .add('ar', 'إدخالات')
    .add('ru', 'записей')
    .add('it', 'voci')
    .add('ro', 'înregistrări')
    .add('es', 'entradas')
    .add('sq', 'hyrje'),


  noData: new TextBundle()
    .add('en', 'no data')
    .add('nl', 'geen data')
    .add('de', 'keine Daten')
    .add('pl', 'brak danych')
    .add('tr', 'veri yok')
    .add('ar', 'لا توجد بيانات')
    .add('ru', 'нет данных')
    .add('it', 'nessun dato')
    .add('ro', 'fără date')
    .add('es', 'sin datos')
    .add('sq', 'pa të dhëna'),

  deleted: new TextBundle()
    .add('en', 'deleted')
    .add('nl', 'verwijderd')
    .add('de', 'gelöscht')
    .add('pl', 'usunięte')
    .add('tr', 'silindi')
    .add('ar', 'تم الحذف')
    .add('ru', 'удалено')
    .add('it', 'eliminati')
    .add('ro', 'șterse')
    .add('es', 'eliminado')
    .add('sq', 'fshirë')
}
