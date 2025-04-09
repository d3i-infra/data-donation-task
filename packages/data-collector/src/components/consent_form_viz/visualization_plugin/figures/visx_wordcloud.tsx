import { Text } from '@visx/text'
import Wordcloud from '@visx/wordcloud/lib/Wordcloud'
import { ParentSize } from '@visx/responsive'
import { ScoredTerm, TextVisualizationData } from '../types'
import { useMemo } from 'react'
import stopwords from './common_stopwords'

interface Props {
  visualizationData: TextVisualizationData
}

interface Word extends ScoredTerm {
  fontSize: number
}

function VisxWordcloud ({ visualizationData }: Props): JSX.Element | null {
  const colors = ['#444', '#1E3FCC', '#4272EF', '#CC9F3F', '#FFCF60']
  const nWords = 100

  const words: Word[] = useMemo(() => {
    const fontRange = [20, 50]
    const words = visualizationData.topTerms.filter((w) => !stopwords.includes(w.text.toLowerCase())).slice(0, nWords)

    let minImportance = words[0].importance
    let maxImportance = words[0].importance
    words.forEach((w) => {
      if (w.importance < minImportance) minImportance = w.importance
      if (w.importance > maxImportance) maxImportance = w.importance
    })

    const [sqrtMin, sqrtMax] = [Math.sqrt(minImportance), Math.sqrt(maxImportance)]
    return words.map((w) => {
      const sqrtImportance = Math.sqrt(w.importance)

      const scale = (sqrtImportance - sqrtMin) / Math.max(sqrtMax - sqrtMin, 0.001)
      const fontSize = scale * (fontRange[1] - fontRange[0]) + fontRange[0]
      return { ...w, fontSize }
    })
  }, [visualizationData, nWords])

  return (
    <ParentSize debounceTime={1000}>
      {(parent) => (
        <Wordcloud
          words={words}
          height={parent.height}
          width={parent.width}
          rotate={0}
          padding={4}
          spiral='rectangular'
          font='Finador-Bold'
          fontSize={(w) => w.fontSize}
          random={() => 0.5}
        >
          {(cloudWords) => {
            return cloudWords.map((w, i: number) => {
              return (
                <Text
                  key={w.text}
                  fill={colors[Math.floor((i / cloudWords.length) * colors.length)]}
                  fontSize={w.size}
                  textAnchor='middle'
                  fontFamily={w.font}
                  transform={`translate(${w.x ?? 0}, ${w.y ?? 0}) rotate(${w.rotate ?? 0})`}
                >
                  {w.text}
                </Text>
              )
            })
          }}
        </Wordcloud>
      )}
    </ParentSize>
  )
}

export default VisxWordcloud
