# Command Protocol

Every interaction between Python and the rest of the system goes through a
**command**. Python yields a command object; `ScriptWrapper.send()` serialises
it to a dict via `.toDict()`; the worker posts it to the JS thread; and the
JS framework handles it and returns a response that Python receives as the
return value of the `yield`.

All command classes live in `packages/python/port/api/commands.py`. Their
TypeScript counterparts are in
`packages/feldspar/src/framework/types/commands.ts`.

---

## Command types

### `CommandUIRender`

Tells the JS framework to render a page and wait for the participant to
interact with it.

```
Python:  yield CommandUIRender(page)
Returns: a Payload* matching the UI component shown
```

| Response type | When returned |
|---|---|
| `PayloadFile` | Participant selected a file |
| `PayloadString` | File delivered via WORKERFS path (legacy) |
| `PayloadTrue` | Participant clicked the "ok" / confirm button |
| `PayloadFalse` | Participant clicked the "cancel" / decline button |
| `PayloadJSON` | Participant submitted a consent form or questionnaire |
| `PayloadVoid` | Error or unexpected state in the renderer |

In practice, `ph.render_page(header, body)` wraps this — you rarely yield
`CommandUIRender` directly.

---

### `CommandSystemDonate`

Sends a key/value pair to the host for storage.

```
Python:  yield CommandSystemDonate(key, json_string)
Returns: PayloadVoid (fire-and-forget, older hosts)
         PayloadResponse (newer hosts with VITE_ASYNC_DONATIONS=true)
```

`PayloadResponse.value` has the shape `{ success: bool, key: str, status: int, error?: str }`.
`port_helpers.handle_donate_result()` normalises both response types into a
single `bool`.

The donation key is conventionally `"{session_id}-{platform_name}"`, e.g.
`"1741234567890-linkedin"`. The `json_string` is whatever the participant
consented to share.

---

### `CommandSystemLog`

Sends a log message to the host. The host forwards it to its logging
infrastructure (Eyra mono routes these to `/api/feldspar/log`).

```
Python:  yield CommandSystemLog(level, message)
Returns: PayloadVoid (always — the response is discarded)
```

Use via `yield from ph.emit_log(level, message)`. Messages **must be
PII-free** — error counts and milestone strings only. See
[Logging](06-logging.md) for the full rules.

---

### `CommandSystemExit`

Signals that the script has finished. The JS engine does not send a
`nextRunCycle` after receiving this — the run loop halts.

```
Python:  yield CommandSystemExit(code, info)
Returns: (nothing — the promise never resolves; the loop stops)
```

`ScriptWrapper.send()` raises `StopIteration` when the generator is
exhausted, and returns `CommandSystemExit(0, "End of script")` automatically.
You should not yield this manually from `FlowBuilder` flows.

---

## Serialisation

Every command class has a `.toDict()` method that produces a plain dict with
a `__type__` field. The worker passes this to the JS thread via
`postMessage()`, which uses the structured clone algorithm (no manual
JSON serialisation needed for most types).

The TypeScript side uses type guard functions (`isCommandSystemLog()`, etc.)
in `commands.ts` to narrow the type before routing.

---

## Response types

| Type | Fields | Produced by |
|---|---|---|
| `PayloadFile` | `value: File` | File input prompt |
| `PayloadString` | `value: string` | WORKERFS file path (legacy) |
| `PayloadTrue` | — | Confirm / ok button |
| `PayloadFalse` | — | Cancel / decline button |
| `PayloadJSON` | `value: string` | Consent form, questionnaire |
| `PayloadVoid` | — | Host acknowledgement (fire-and-forget) |
| `PayloadResponse` | `value: { success, key, status, error? }` | Host donation result (async mode) |

---

→ [FlowBuilder](04-flowbuilder.md) — how Python uses these commands to drive a donation flow
