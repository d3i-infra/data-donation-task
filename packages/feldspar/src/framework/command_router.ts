import { Command, Response, isCommandSystem, isCommandSystemDonate, isCommandSystemExit, isCommandUI, CommandUI, CommandSystem } from './types/commands'
import { CommandHandler, Bridge } from './types/modules'
import ReactEngine from './visualization/react/engine'

export default class CommandRouter implements CommandHandler {
  bridge: Bridge
  visualizationEngine: ReactEngine

  constructor (bridge: Bridge, visualizationEngine: ReactEngine) {
    this.bridge = bridge
    this.visualizationEngine = visualizationEngine
  }

  async onCommand (command: Command): Promise<Response> {
    if (isCommandSystem(command)) {
      return this.onCommandSystem(command)
    } else if (isCommandUI(command)) {
      return await new Promise<Response>((resolve, reject) => {
        this.onCommandUI(command, resolve)
      })
    } else {
      throw new TypeError('[CommandRouter] Unknown command' + JSON.stringify(command))
    }
  }

  // Awaiting bridge.send() for donate commands is the pattern from eyra/feldspar
  // PR #612 (draft, feature/live_error_handling) and what-if-horizon commit 0020453.
  // bridge.send() always awaits the host's DonateSuccess/DonateError for a donate
  // command (protocol from eyra/mono commit f1395c378; both monos reply
  // unconditionally), so this method returns PayloadResponse and Python can inspect
  // the outcome. A bridge that resolves with void — e.g. a stub — still yields
  // PayloadVoid through the fall-through below.
  async onCommandSystem (command: CommandSystem): Promise<Response> {
    if (isCommandSystemExit(command)) {
      this.bridge.send(command)
      console.log('[CommandRouter] Application exit')
      // Never resolves — halts the run cycle intentionally
      return new Promise<Response>(() => {})
    }

    if (isCommandSystemDonate(command)) {
      const result = await this.bridge.send(command)
      if (result !== undefined) {
        console.log('[CommandRouter] Donate result:', result)
        return { __type__: 'Response', command, payload: { __type__: 'PayloadResponse', value: result } }
      }
    } else {
      await this.bridge.send(command)
    }

    return { __type__: 'Response', command, payload: { __type__: 'PayloadVoid', value: undefined } }
  }

  onCommandUI (command: CommandUI, resolve: (response: Response) => void): void {
    this.visualizationEngine.render(command)
      .then((response) => {
        if (!response || !response.__type__) {
          console.error('[CommandRouter] Invalid response:', response);
          resolve({
            __type__: 'Response',
            command,
            payload: { __type__: 'PayloadVoid', value: undefined }
          });
        } else {
          resolve(response);
        }
      })
      .catch((error) => {
        console.error('[CommandRouter] Error:', error);
        resolve({
          __type__: 'Response',
          command,
          payload: { __type__: 'PayloadVoid', value: undefined }
        });
      });
  }
}
