import { addFiles } from './select'
const f = (name: string, size = 10, lastModified = 1) =>
  new File([new ArrayBuffer(size)], name, { lastModified })

test('appends new files preserving order', () => {
  const { files, duplicates } = addFiles([f('a.zip')], [f('b.zip')])
  expect(files.map((x) => x.name)).toEqual(['a.zip', 'b.zip'])
  expect(duplicates).toEqual([])
})
test('drops exact duplicates (name+size+lastModified) and reports them', () => {
  const { files, duplicates } = addFiles([f('a.zip', 10, 1)], [f('a.zip', 10, 1), f('c.zip')])
  expect(files.map((x) => x.name)).toEqual(['a.zip', 'c.zip'])
  expect(duplicates).toEqual(['a.zip'])
})
test('same name with different size or mtime is kept', () => {
  const { files } = addFiles([f('a.zip', 10, 1)], [f('a.zip', 11, 1), f('a.zip', 10, 2)])
  expect(files).toHaveLength(3)
})
