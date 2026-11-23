# Optional Binance application

This module owns the deterministic Binance Spot BTCUSDT L2 application, its SBE schema 401, and its book/replay
tests. Generic `protocol`, `client` and `sequencer` artifacts have no dependency on it.

Select it explicitly when constructing the service or pure core:

```java
import spine.examples.binance.BinanceApplication;
import spine.sequencer.SequencerCore;
import spine.sequencer.SequencerService;

var service = new SequencerService(BinanceApplication.IDENTITY, new BinanceApplication());
service.configure(consensusContext, serviceContext); // before launching native Cluster components
// Or, for a separately owned offline replay:
var core = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
```

Each instance belongs to one owner. The default constructors use `Application.PASS_THROUGH`, which retains original
observations without interpreting market data. Restore Binance snapshots with the same application and identity. Its Spine-owned schema 401 starts at version 1.

Run `./gradlew :examples-binance:test`. The retained-corpus tool is
`:examples-binance:binanceBookReplay`; the local recorded-path diagnostic is `:examples-binance:marketBookProfile`.
See the [application contract](../docs/application-contract.md) and [market acceptance](../docs/market-state-result.md).
