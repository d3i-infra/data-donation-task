import { Command, CommandSystem, Response } from './commands'
import { LogEntry } from '../logging'

// Structured result returned by the host after processing a CommandSystemDonate.
// Introduced by eyra/mono commit f1395c378 (Jan 20 2026) / eyra/feldspar PR #612
// (draft, feature/live_error_handling). Adopted by what-if-horizon in commit
// 0020453 "wait for donation result (based on PR 612)".
export interface ResponseSystemDonate {
  success: boolean
  key: string
  status: number
  error?: string
}

// Bridge.send is async: it resolves with a ResponseSystemDonate for donate commands
// — the host acknowledges every donation via MessageChannel — and with void for all
// other commands. Pattern from eyra/feldspar PR #612.
export interface Bridge {
  send: (command: CommandSystem) => Promise<ResponseSystemDonate | void>
  sendLogs: (entries: LogEntry[]) => void
}

export interface CommandHandler {
  onCommand: (command: Command) => Promise<Response>
}
