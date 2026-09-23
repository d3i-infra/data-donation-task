import { CommandSystem, isCommandSystem, isCommandSystemDonate } from './framework/types/commands'
import { Bridge, ResponseSystemDonate } from './framework/types/modules'
import { LogEntry } from './framework/logging'

// The bridge always awaits a DonateSuccess/DonateError response from the host
// before resolving a donate command. Both monos (d3i-infra/mono and eyra/mono)
// attempt a reply on every handled path of `donate_via_api` in
// core/assets/js/feldspar_app.js — including network errors and non-ok
// responses — so a failed upload comes back as DonateError rather than silence.
// This assumes a host carrying the donate-ack protocol (d3i-infra/mono
// bbfcbffbd, 2026-02-02); against an older mono no reply is ever sent and the
// participant waits forever. Note the host's own send is guarded
// (`if (this.channel && this.channel.port1)`), which is why updatePort() must
// keep failing pending donations when the channel is replaced.
// D3I-specific addition — upstream eyra/feldspar's `send()` returns void and
// ignores the acknowledgment.

// Response types sent back from feldspar_app.js via MessageChannel.
// Protocol introduced in eyra/mono commit f1395c378 "Refactor data donation to
// use HTTP upload instead of WebSocket" (Jan 20 2026), which replaced LiveView
// pushEvent with an HTTP POST to /api/feldspar/donate and added DonateSuccess/
// DonateError replies via channel.port1.postMessage().
// Async handling of these types is the subject of eyra/feldspar PR #612
// "Feature/live error handling" (draft as of Feb 2026, branch feature/live_error_handling,
// tip 94ed016). What-if-horizon adopted this in commit 0020453 "wait for donation
// result (based on PR 612)" (Feb 18 2026).
export interface DonateSuccess {
  __type__: 'DonateSuccess'
  key: string
  status: number
}

export interface DonateError {
  __type__: 'DonateError'
  key: string
  status: number
  error: string
}

export type DonateResponse = DonateSuccess | DonateError

export function isDonateResponse (data: any): data is DonateResponse {
  return data && (data.__type__ === 'DonateSuccess' || data.__type__ === 'DonateError')
}

interface PendingDonation {
  resolve: (result: ResponseSystemDonate) => void
}

export class LiveBridge implements Bridge {
  port: MessagePort
  private pendingDonations: Map<string, PendingDonation> = new Map()

  // Tracks the single active bridge so we can update its port when the host
  // re-initializes the MessageChannel. This replaces the old `static initialized`
  // boolean, which was correct for preventing double-init but prevented the port
  // update needed to fix the Firefox channel-mismatch bug (see updatePort below).
  // Firefox fix first applied in daniellemccool/what-if-data-donation-test
  // commit 40af878 "Fix Firefox channel-mismatch hang and platform-string bug".
  static currentBridge: LiveBridge | null = null

  constructor (port: MessagePort) {
    this.port = port
    this.setupResponseListener()
  }

  private setupResponseListener (): void {
    this.port.onmessage = (event) => {
      if (isDonateResponse(event.data)) {
        this.handleDonateResponse(event.data)
      } else {
        this.log('info', 'received unknown message', event.data)
      }
    }
  }

  // feldspar_app.js calls setupChannel() for both the iframe `onload` event and
  // the `app-loaded` postMessage. The guard there only blocks `onload` from
  // replacing an existing channel — `app-loaded` has no guard. In Firefox,
  // `onload` fires (creating Channel A, which LiveBridge receives) BEFORE
  // `app-loaded` is delivered to the host. When `app-loaded` then arrives it
  // replaces `this.channel` with Channel B and sends a second `live-init` with
  // port2_B. With the old `static initialized` flag that message was ignored,
  // leaving LiveBridge on port2_A while the host used Channel B. Every call to
  // sendDonateResponse() then posted to port1_B → port2_B (no listener) → hang.
  //
  // Fix: accept the new port when a subsequent `live-init` arrives and update
  // the response listener. Any donations waiting on the old port are failed
  // immediately — they would have hung forever anyway.
  updatePort (newPort: MessagePort): void {
    this.log('info', 'Host re-initialized MessageChannel — updating port')
    this.port = newPort
    this.setupResponseListener()
    for (const [key, pending] of this.pendingDonations) {
      this.log('error', `Failing pending donation ${key}: channel replaced by host`)
      pending.resolve({ success: false, key, status: 0, error: 'Channel re-initialized by host' })
    }
    this.pendingDonations.clear()
  }

  private handleDonateResponse (response: DonateResponse): void {
    const pending = this.pendingDonations.get(response.key)

    if (response.__type__ === 'DonateSuccess') {
      console.log('[LiveBridge] DonateSuccess:', { key: response.key, status: response.status })
      if (pending) {
        pending.resolve({ success: true, key: response.key, status: response.status })
      }
    } else {
      console.error('[LiveBridge] DonateError:', {
        key: response.key,
        status: response.status,
        error: response.error
      })
      if (pending) {
        pending.resolve({
          success: false,
          key: response.key,
          status: response.status,
          error: response.error
        })
      }
    }

    this.pendingDonations.delete(response.key)
    this.log('info', `Donation completed, pending: ${this.pendingDonations.size}`)
  }

  static create (window: Window, callback: (bridge: Bridge, locale: string) => void): void {
    window.addEventListener('message', (event) => {
      console.log('MESSAGE RECEIVED', event)
      if (event.data.action === 'live-init') {
        const newPort = event.ports[0]
        const locale = event.data.locale

        if (LiveBridge.currentBridge === null) {
          // First live-init: create the bridge and start the application.
          console.log('LOCALE', locale)
          const bridge = new LiveBridge(newPort)
          LiveBridge.currentBridge = bridge
          callback(bridge, locale)
        } else {
          // Subsequent live-init: host replaced its MessageChannel.
          // Update the existing bridge's port instead of ignoring the message.
          LiveBridge.currentBridge.updatePort(newPort)
        }
      }
    })
  }

  sendLogs (entries: LogEntry[]): void {
    entries.forEach(entry => {
      this.port.postMessage({
        __type__: 'CommandSystemLog',
        level: entry.level,
        message: entry.message,
        json_string: JSON.stringify({ level: entry.level, message: entry.message }),
      })
    })
  }

  async send (command: CommandSystem): Promise<ResponseSystemDonate | void> {
    if (!isCommandSystem(command)) {
      this.log('error', 'received unknown command', command)
      return
    }

    this.log('info', 'send', command)

    if (isCommandSystemDonate(command)) {
      return new Promise<ResponseSystemDonate>((resolve) => {
        this.pendingDonations.set(command.key, { resolve })
        this.log('info', `Donation started, pending: ${this.pendingDonations.size}`)
        this.port.postMessage(command)
      })
    }

    this.port.postMessage(command)
  }

  private log (level: 'info' | 'error', ...message: any[]): void {
    const logger = level === 'info' ? console.log : console.error
    logger('[LiveBridge]', ...message)
  }
}
