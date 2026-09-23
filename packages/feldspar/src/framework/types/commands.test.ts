import { PayloadFiles, PayloadResolved } from './commands'

test('PayloadFiles carries a File array and joins PayloadResolved', () => {
  const p: PayloadFiles = { __type__: 'PayloadFiles', value: [new File(['x'], 'a.zip')] }
  const r: PayloadResolved = p   // compile-time membership check
  expect(r.__type__).toBe('PayloadFiles')
})
