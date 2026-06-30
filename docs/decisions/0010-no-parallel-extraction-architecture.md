---
adr_id: "0006"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-16 17:27:31"
    - author: Danielle McCool
      comment: "2"
      date: "2026-03-17 13:18:38"
    - author: Danielle McCool
      comment: "3"
      date: "2026-03-17 14:34:04"
links:
    precedes: []
    succeeds: []
status: decided
tags:
    - extraction
    - flowbuilder
    - consolidation
    - donation-flows
title: Consolidate donation_flows and platforms into single extraction architecture
---

## <a name="question"></a> Context and Problem Statement

Two parallel extraction systems exist: donation_flows/ (data-driven entries pattern from what-if — active in script.py) and platforms/ (FlowBuilder template method with DDP_CATEGORIES — d3i-infra standard but unwired). The project's core principle is data minimization: only extract what researchers need. donation_flows/ auto-extracts everything (180 Facebook tables); platforms/ provides curated hand-written extraction functions. FlowBuilder was designed by trbKnl to standardize data donation flows and is used across the d3i ecosystem. How should these be consolidated into a single standard?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> FlowBuilder control flow + data-driven extraction + DDP_CATEGORIES validation
2. <a name="option-2"></a> Keep donation_flows/ pattern with DDP_CATEGORIES validation bolted on
3. <a name="option-3"></a> Keep both systems — let researchers choose
4. <a name="option-4"></a> FlowBuilder standard with hand-written extraction library — remove donation_flows/

## <a name="criteria"></a> Decision Drivers
Feldspar alignment: FlowBuilder.start_flow() mirrors eyra's expected generator protocol — the retry/consent/donation control flow should be framework-level, not per-script
Researcher usability: researchers add platforms by writing extraction logic, not UI boilerplate — the data-driven entries pattern (donation_flows/) lets a researcher define a table in 3 lines vs 15+ lines of PropsUI construction (platforms/)
ADR compliance: extraction/AD0001 requires FlowBuilder; python-architecture/AD0003 requires helper-based UI construction — the consolidated system must satisfy both
Data completeness: donation_flows/ extracts 180 Facebook tables vs 26 in platforms/ — a consolidation must not lose coverage
Validation robustness: DDP_CATEGORIES (platforms/) validates against known file lists with threshold matching; donation_flows/ uses simple pattern matching (has_file_in_zip) which accepts invalid DDPs
Retry flow correctness: FlowBuilder.start_flow() has a bug at line 76 — yields retry prompt but never checks the response, so retries skip back to consent instead of re-prompting for file
Error observability: donation_flows/ uses logging.exception() with full traceback and structured DataFrame export; platforms/ uses logger.error() without traceback and no export mechanism
Code maintainability: donation_flows/ is 59 lines per platform (data-driven); platforms/ is 1000+ lines per platform (hand-written extraction functions)
Data minimization: the project only extracts what researchers need — auto-extraction (180 tables) conflicts with this principle; hand-written functions are intentional selections
Ecosystem adoption: FlowBuilder is used in d3i-infra (9 platforms) and dd-vu-2026 (7 production platforms) — it is the de facto d3i standard
### Pros and Cons

**FlowBuilder control flow + data-driven extraction + DDP_CATEGORIES validation**
* Good, because satisfies both extraction/AD0001 (FlowBuilder) and python-architecture/AD0003 (helper-based UI)
* Good, because preserves 180-table data completeness from donation_flows/ entries pattern
* Good, because adopts DDP_CATEGORIES validation from platforms/ replacing weak pattern matching
* Good, because researchers add platforms via entries dict (3 lines per table) not PropsUI boilerplate
* Good, because FlowBuilder.start_flow() centralizes retry/consent/donation — aligned with feldspar generator protocol
* Neutral, because requires fixing FlowBuilder retry bug (line 76) and wiring structured logging into FlowBuilder
* Bad, because requires migrating 5 platforms from donation_flows/ entries format into FlowBuilder subclasses with DDP_CATEGORIES
* Bad, because the 5 extra platforms in platforms/ (ChatGPT, LinkedIn, Netflix, WhatsApp, X) need their hand-written extraction functions converted to entries format or kept as-is

**Keep donation_flows/ pattern with DDP_CATEGORIES validation bolted on**
* Good, because minimal migration — donation_flows/ already works and is wired to script.py
* Good, because preserves data-driven entries pattern and helper-based UI construction
* Good, because adding DDP_CATEGORIES to donation_flows/ validation is straightforward
* Bad, because violates extraction/AD0001 — no FlowBuilder, control flow stays in script.py
* Bad, because script.py orchestration is not aligned with feldspar's generator protocol expectations
* Bad, because the 5 extra platforms (ChatGPT, LinkedIn, Netflix, WhatsApp, X) only exist in platforms/ and would need rewriting
* Bad, because retry/consent/donation flow duplicated per-script instead of centralized

**Keep both systems — let researchers choose**
* Good, because no migration work needed
* Bad, because two parallel architectures confuse contributors and violate ADR intent
* Bad, because bug fixes and flow improvements must be applied in two places
* Bad, because new researchers don't know which pattern to follow

**FlowBuilder standard with hand-written extraction library — remove donation_flows/**
* Good, because realizes trbKnl's original goal of standardizing data donation flows
* Good, because aligns with data minimization — researchers select specific tables, not auto-extract everything
* Good, because satisfies extraction/AD0001 (FlowBuilder) and python-architecture/AD0003 (helper-based UI)
* Good, because single architecture — no ambiguity for contributors or new researchers
* Good, because FlowBuilder is already the de facto d3i standard (9 platforms in d3i-infra, 7 in dd-vu-2026)
* Good, because DDP_CATEGORIES validation is preserved
* Neutral, because requires fixing FlowBuilder retry bug and adding missing flow steps (safety, donation result handling)
* Bad, because fewer tables per platform than donation_flows/ (26 vs 180 for Facebook) — researchers needing more coverage add extraction functions manually
* Bad, because platforms/ code is more verbose (1000+ LOC per platform vs 59)

## <a name="outcome"></a> Decision Outcome
We decided for [Option 4](#option-4) because: FlowBuilder is the standard per the extraction consolidation design spec. donation_flows/ has been removed. All 9 platforms use FlowBuilder subclasses with hand-written extraction functions and DDP_CATEGORIES validation.

## <a name="comments"></a> Comments
<a name="comment-3"></a>3. (2026-03-17 14:34:04) Danielle McCool: marked decision as decided
