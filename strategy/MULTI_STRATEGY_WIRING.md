# Multi-strategy registry

Run multiple strategies concurrently, each on its own configurable symbols and
ladders, computing shared chains once.

## Pieces

1. **`strategy/ladder_registry.py`** — `Ladder` + `LadderRegistry`. Ladders as a
   registry (generalizing the fixed Set A / Set B enum). A ladder is a named,
   ordered timeframe list; `level_of` is ladder-relative (2m is level 5 in A,
   level 3 in B — preserved). A and B are pre-registered.

2. **`strategy/registry.py`** — `Strategy` (name, entry, exit, symbols, ladders)
   + `StrategyRegistry`. Reads `config.STRATEGIES`, instantiates enabled
   strategies, answers "which strategies trade symbol S" and "which ladders must
   be computed" (the union — compute-once).

3. **`strategy/multi_runtime.py`** — `MultiStrategyRuntime`. Fans each event to
   every enabled strategy on that symbol; tracks open positions by
   `(strategy, position_key)`; routes exits back to the owning strategy.

4. **`migrations/011_signals_strategy.sql`** — adds the `strategy` column to
   `trade_signals` (additive, non-breaking).

5. **`config.STRATEGIES` / `config.EXTRA_LADDERS`** — the config.

## Compute-once across strategies

The shared unit is the **ladder**, not the strategy. A chain is a ladder-relative
object (its level only means something within its ladder), so the chain machine
runs once per `(symbol, ladder)`. Two strategies that both read ladder B share
B's chains for free. `registry.all_ladders()` is the union the system computes;
`registry.strategies_reading_ladder(name)` is who to notify.

This is the answer to "compute chains once, let each strategy monitor the ladders
it cares about": true, because ladders — not strategies — are the compute unit.
What you cannot do is compute a *single* chain set both strategies read if they
use *different* ladders — different ladders are genuinely different chains.

## Multiple strategies on one symbol

Two strategies can hold concurrent positions on the same symbol. They share one
`ChainView` (chain state is the market's), but positions are namespaced by
strategy:

- runtime tracks by `strategy_name/chain_id:side`
- the emitted signal's `signal_id` folds in the strategy (via `SignalWriter`)
- the `trade_signals.strategy` column records which strategy produced each row

So two strategies on the same chain produce two distinct rows, not one deduped
row. (Proven in `test_strategy_signals_integration.py` against real Postgres.)

## Contract change (coordinate with the consuming team)

`trade_signals` gains a `strategy` column. It's **additive and non-breaking**:
nullable-with-default, so existing INSERTs and consumers that ignore it work
unchanged. But the position-matching contract changes: `(strategy, position_key)`
names one position, not `position_key` alone — a consumer matching an EXIT to
its ENTER must match on both. Flag this to the team.

## Wiring (in signal_process, replacing the single-strategy runtime)

```python
from strategy.ladder_registry import default_registry
from strategy.registry import StrategyRegistry
from strategy.multi_runtime import MultiStrategyRuntime
from execution.signal_writer import SignalWriter, SignalWriterHooks

# 1. Ladders: built-in A/B plus any extras from config.
ladders = default_registry()
for name, tfs in config.EXTRA_LADDERS.items():
    ladders.register(name, tfs)

# 2. Strategy registry: register factories, build from config.
registry = StrategyRegistry()
registry.register_factory("two_ladder", make_two_ladder)   # per-strategy factory
# ... register other strategy factories ...

view = ChainView()   # shared across strategies
def make_two_ladder(*, name, symbols, ladders, params, view, insert_signal):
    writer = SignalWriter(SignalWriterHooks(insert_signal=insert_signal), strategy=name)
    return Strategy(
        name=name,
        entry=TwoLadderEntry(view, writer),
        exit=TwoLadderExit(view, writer),
        symbols=symbols, ladders=ladders,
    )

registry.build_from_config(
    config.STRATEGIES,
    all_symbols=config.all_tracked_symbols(),
    view=view, insert_signal=make_insert_signal(pool),
)

# 3. Runtime over the registry.
runtime = MultiStrategyRuntime(view, registry, runtime_hooks, locks=router._locks)
await runtime.start()
```

The feeds must stream the union of all enabled strategies' ladder timeframes:
`ladders.timeframes_for_ladders(registry.all_ladders())` — feed this into the
per-asset-class timeframe config.

## What was deliberately NOT built (YAGNI)

- **Live strategy enable/disable via DB** (like `trade_enabled`). Not requested;
  strategy activation is a config edit + restart for now. The DB-toggle pattern
  is available to copy if it's wanted later.
- **A strategy loader / DSL.** Strategies are registered factories, not
  dynamically loaded. One strategy exists today; the abstraction stays a
  protocol + dict until a second strategy shows what actually varies.
```
