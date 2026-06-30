---
adr_id: "0004"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-17 13:24:17"
    - author: Danielle McCool
      comment: "2"
      date: "2026-03-17 13:24:17"
links:
    precedes:
        - "0005"
    succeeds: []
status: decided
tags:
    - generator-protocol
    - termination
    - host-integration
title: Flow termination via generator exhaustion not explicit exit command
---

## <a name="question"></a> Context and Problem Statement

When a data donation flow completes the Python generator must signal termination to the TypeScript host. Two mechanisms exist: yielding CommandSystemExit explicitly or letting the generator exhaust (StopIteration) which ScriptWrapper catches and converts to CommandSystemExit. Eyra's architecture uses generator exhaustion: script.py yields a final page and returns; main.py handles the protocol. Which approach should FlowBuilder and script.py use?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Generator exhaustion with ScriptWrapper conversion
2. <a name="option-2"></a> Explicit CommandSystemExit at end of script.py
3. <a name="option-3"></a> FlowBuilder yields exit after each platform

## <a name="criteria"></a> Decision Drivers
Eyra's feldspar uses generator exhaustion — script.py returns after the last yield and ScriptWrapper converts StopIteration to CommandSystemExit
dd-vu-2026's script.py previously ended with yield CommandUIRender(PropsUIPageEnd()) and returned — no explicit CommandSystemExit; AD0005 removed that end-page yield, so today script.py's last yield is the "Study complete" log and the generator then returns
FlowBuilder should not terminate the study — it handles one platform; script.py handles the study lifecycle
Mixing explicit exit commands with generator exhaustion creates ambiguity about who owns termination
### Pros and Cons

**Generator exhaustion with ScriptWrapper conversion**
* Good, because aligned with Eyra's architecture — ScriptWrapper already handles this
* Good, because FlowBuilder can simply return without knowing about exit protocol
* Good, because script.py yields its last per-platform command and returns — clean separation
* Good, because single termination path — no ambiguity about who calls exit
* Neutral, because requires understanding the generator protocol to debug termination issues

**Explicit CommandSystemExit at end of script.py**
* Good, because termination is visible in script.py code
* Bad, because FlowBuilder.start_flow() previously yielded ph.exit() — mixing patterns
* Bad, because if script.py forgets to yield exit ScriptWrapper does it anyway — redundant


## <a name="outcome"></a> Decision Outcome
We decided for [Option 1](#option-1) because: This is how Eyra designed it: ScriptWrapper (main.py) catches StopIteration and returns CommandSystemExit(0, 'End of script'). script.py's last yield is a log milestone ("Study complete") and the generator then returns. FlowBuilder.start_flow() returns after completing a platform — it never yields exit commands. The host (mono) receives the exit command from ScriptWrapper and renders its own completion UI (finished_view / checkmark); see AD0005 for the decision to drop the in-iframe EndPage. This is a retrospective ADR documenting the intended pattern that was inconsistently followed.

## <a name="comments"></a> Comments
<a name="comment-2"></a>2. (2026-03-17 13:24:17) Danielle McCool: More Information:
See python-architecture/AD0005 for the generator protocol ADR. See main.py ScriptWrapper.send() line 88-89 for the StopIteration → CommandSystemExit conversion. See python-architecture/AD0006 for the consolidation design that enforces this pattern.
