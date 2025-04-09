import { extractUrlDomain, getTableColumn, tokenize } from './util'
import { TextVisualizationData, TextVisualization, ScoredTerm, Table } from '../types'

interface VocabularyStats {
  value: number
  docFreq: number
}

export async function prepareTextData (table: Table, visualization: TextVisualization): Promise<TextVisualizationData> {
  const visualizationData: TextVisualizationData = {
    type: visualization.type,
    topTerms: []
  }

  if (table.body.rows.length === 0) return visualizationData

  const texts = getTableColumn(table, visualization.textColumn)
  const values = visualization.valueColumn != null ? getTableColumn(table, visualization.valueColumn) : null

  const vocabulary = getVocabulary(texts, values, visualization)
  visualizationData.topTerms = getTopTerms(vocabulary, texts.length, 200)

  return visualizationData
}

function getVocabulary (
  texts: string[],
  values: string[] | null,
  visualization: TextVisualization
): Record<string, VocabularyStats> {
  const vocabulary: Record<string, VocabularyStats> = {}

  for (let i = 0; i < texts.length; i++) {
    if (texts?.[i] == null) continue
    const text = texts[i]
    const tokens = visualization.tokenize != null && visualization.tokenize ? tokenize(text) : [text]

    const seen = new Set<string>()
    for (let token of tokens) {
      if (visualization.extract === 'url_domain') token = extractUrlDomain(token)
      if (vocabulary[token] === undefined) vocabulary[token] = { value: 0, docFreq: 0 }
      if (!seen.has(token)) {
        vocabulary[token].docFreq += 1
        seen.add(token)
      }

      const v = values != null ? Number(values[i]) ?? 1 : 1
      if (!isNaN(v)) vocabulary[token].value += v
    }
  }
  return vocabulary
}

function getTopTerms (vocabulary: Record<string, VocabularyStats>, nDocs: number, topTerms: number): ScoredTerm[] {
  return Object.entries(vocabulary)
    .map(([text, stats]) => {
      const tf = stats.value
      const idf = Math.log(nDocs / stats.docFreq)
      return { text, value: stats.value, importance: tf * idf }
    })
    .sort((a, b) => b.importance - a.importance)
    .slice(0, topTerms)
}
