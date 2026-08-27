import WorkerProcessingEngine from './worker_engine'
import { CommandHandler } from '../types/modules'

const commandHandler: CommandHandler = {
  onCommand: jest.fn(),
}

describe('WorkerProcessingEngine', () => {
  it('posts the #960 data dict with sessionId, locale, and platform', () => {
    const worker = { postMessage: jest.fn(), onmessage: null } as any
    const engine = new WorkerProcessingEngine('123', 'nl', worker, commandHandler, undefined, 'example')
    engine.firstRunCycle()
    expect(worker.postMessage).toHaveBeenCalledWith({
      eventType: 'firstRunCycle',
      data: { sessionId: '123', locale: 'nl', platform: 'example' },
    })
  })

  // The platform prop is the only source (ADR-0004): with no platform argument the
  // handshake carries `undefined`, and the Python layer raises — nothing substitutes
  // a value here.
  it('sends an undefined platform when the constructor was given none', () => {
    const worker = { postMessage: jest.fn(), onmessage: null } as any
    const engine = new WorkerProcessingEngine('123', 'nl', worker, commandHandler, undefined)
    engine.firstRunCycle()
    expect(worker.postMessage).toHaveBeenCalledWith({
      eventType: 'firstRunCycle',
      data: { sessionId: '123', locale: 'nl', platform: undefined },
    })
  })
})
