# Code Review Protocol

When reviewing non-trivial changes to log-courier, work through this
protocol systematically. Use TodoWrite to track each item.

## 1. Understand the System Loop

Log-courier operates in cycles: Discovery → Process → State Update → Next Discovery

For ANY change, trace:
- [ ] How does this change affect one complete cycle?
- [ ] What happens on the NEXT cycle after this change?
- [ ] What happens on the THIRD cycle? (catches accumulation bugs)
- [ ] Draw the state at T0, T1, T2 if unclear

## 2. Identify Data Flows

For any state that's modified:
- [ ] List ALL writers to this state
- [ ] List ALL readers of this state
- [ ] When is state written vs. when is it read?
- [ ] Is there a visibility delay? What happens during that window?

## 3. Check Dependent Code

Changes affect more than what's modified:
- [ ] What UNCHANGED code depends on the modified behavior?
- [ ] Search for functions/components that read the modified state
- [ ] Do those components have assumptions that are now violated?
- [ ] Check callers, not just callees

## 4. Verify System Invariants

Identify statements that must ALWAYS be true:
- [ ] "Processed batches are never re-discovered"
- [ ] "Offsets are monotonic"
- [ ] "Offsets are never committed before their corresponding S3 upload succeeds"
- [ ] "At-least-once delivery"
- [ ] Does this change break any invariants?

## 5. Test Coverage Analysis

- [ ] Do tests cover the full system loop (multiple cycles)?
- [ ] Do tests verify invariants explicitly?
- [ ] What scenarios are NOT tested?
- [ ] Would tests catch this if behavior breaks?

## 6. Timing and Concurrency

- [ ] What happens if operations are delayed?
- [ ] What happens if operations are reordered?
- [ ] Are there race conditions between components?
- [ ] Does eventual consistency cause issues?
