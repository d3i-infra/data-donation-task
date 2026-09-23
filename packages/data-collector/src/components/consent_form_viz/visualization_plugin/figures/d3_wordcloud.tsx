import { ReactElement, useEffect, useMemo, useRef } from "react"
import cloud from "d3-cloud"
import stopwords from "./common_stopwords"
import { TextVisualizationData } from "../types"

type Word = { text: string; value: number }

// d3-cloud mutates each word in place, adding x/y/rotate once layout
// completes — they don't exist on the objects passed into .words().
type LayoutWord = { text: string; size: number; x?: number; y?: number; rotate?: number }
type LayoutCloud = ReturnType<typeof cloud<LayoutWord>>

interface Props {
  visualizationData: TextVisualizationData
  nWords?: number
}

const COLORS = ["#444", "#1E3FCC", "#4272EF", "#CC9F3F", "#FFCF60"] as const
const FONT_FAMILY = "Finador-Bold"
const FONT_SIZES: [number, number] = [20, 50]

function Wordcloud({ visualizationData, nWords = 100 }: Props): ReactElement | null {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const svgRef = useRef<SVGSVGElement | null>(null)

  // derive words from visualizationData
  const words: Word[] = useMemo(() => {
    if (!visualizationData?.topTerms?.length) return []
    return visualizationData.topTerms
      .filter(w => !stopwords.includes(w.text.toLowerCase()))
      .slice(0, nWords)
      .map(w => ({ text: w.text, value: w.importance }))
  }, [visualizationData, nWords])

  useEffect(() => {
    if (!containerRef.current || !svgRef.current || words.length === 0) return

    const container = containerRef.current
    const parent = container.parentElement || container
    const svg = svgRef.current

    let activeLayout: LayoutCloud | undefined
    let raf = 0
    let curW = 0
    let curH = 0

    const values = words.map(d => d.value)
    const minValue = Math.min(...values)
    const maxValue = Math.max(...values)
    const span = maxValue - minValue || 1
    const scaleSize = (v: number) =>
      FONT_SIZES[0] + Math.sqrt((v - minValue) / span) * (FONT_SIZES[1] - FONT_SIZES[0])

    const stopLayout = () => {
      const l = activeLayout as any
      if (l?.stop) l.stop()
      activeLayout = undefined
    }

    const draw = (placed: LayoutWord[]) => {
      const g = document.createElementNS("http://www.w3.org/2000/svg", "g")
      g.setAttribute("transform", `translate(${curW / 2},${curH / 2})`)
      placed.forEach((word, idx) => {
        const t = document.createElementNS("http://www.w3.org/2000/svg", "text")
        t.setAttribute("font-size", `${word.size}`)
        t.setAttribute("font-family", FONT_FAMILY)
        t.setAttribute("font-weight", "bold")
        t.setAttribute("fill", COLORS[idx % COLORS.length])
        t.setAttribute("text-anchor", "middle")
        t.setAttribute("transform", `translate(${word.x},${word.y})`)
        t.textContent = word.text
        g.appendChild(t)
      })
      svg.appendChild(g)
    }

    const render = (w: number, h: number) => {
      if (w <= 0 || h <= 0) return
      curW = w
      curH = h

      svg.innerHTML = ""
      svg.setAttribute("viewBox", `0 0 ${w} ${h}`)
      svg.setAttribute("preserveAspectRatio", "xMidYMid meet")

      stopLayout()

      const layout = cloud<LayoutWord>()
        .size([w, h])
        .words(words.map(d => ({ text: d.text, size: scaleSize(d.value) })))
        .padding(4)
        .rotate(() => 0)
        .font(FONT_FAMILY)
        .fontWeight("bold")
        .fontSize(d => d.size)
        .random(() => 0.5)
        .spiral("rectangular")
        .timeInterval(10)
        .on("end", draw)

      activeLayout = layout
      layout.start()
    }

    const ro = new ResizeObserver(entries => {
      const cr = entries[0]?.contentRect
      const w = Math.round(cr.width)
      const h = Math.max(240, Math.round(cr.height) || Math.round(w * 0.6))

      cancelAnimationFrame(raf)
      raf = requestAnimationFrame(() => {
        stopLayout()
        render(w, h)
      })
    })

    ro.observe(parent)

    // initial pass
    const initW = parent.clientWidth
    const initH = Math.max(240, parent.clientHeight || Math.round(initW * 0.6))
    render(initW, initH)

    return () => {
      ro.disconnect()
      cancelAnimationFrame(raf)
      stopLayout()
    }
  }, [words])

  if (words.length === 0) return null

  return (
    <div ref={containerRef} style={{ width: "100%", height: "100%", minHeight: 240 }} >
      <svg ref={svgRef} style={{ width: "100%", height: "100%" }} />
    </div>
  )
}

export default Wordcloud

