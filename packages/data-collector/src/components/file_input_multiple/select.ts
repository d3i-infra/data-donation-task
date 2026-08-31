// Pure file-selection logic for the multi-file input: merging a newly picked
// batch into the current selection, deduplicating on (name, size, lastModified)
// so re-picking the same file (e.g. re-opening the OS file dialog) is a no-op
// rather than a second entry, and reporting which names were dropped so the
// component can surface a notice instead of silently discarding a pick.
const key = (f: File): string => `${f.name} ${f.size} ${f.lastModified}`

export function addFiles(current: File[], incoming: File[]): { files: File[]; duplicates: string[] } {
  const seen = new Set(current.map(key))
  const files = [...current]
  const duplicates: string[] = []
  for (const f of incoming) {
    if (seen.has(key(f))) { duplicates.push(f.name); continue }
    seen.add(key(f)); files.push(f)
  }
  return { files, duplicates }
}
