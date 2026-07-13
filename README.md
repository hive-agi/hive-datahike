# hive-datahike

Datahike (temporal Datalog) implementation of the hive KG storage SPI.

`src/hive_datahike/kg/store.clj` implements
`hive-spi.kg.protocol/IKGStore` + `IPersistentKGStore` + `ITemporalKGStore`
on top of [datahike](https://github.com/replikativ/datahike). It depends
only on `hive-spi`, `hive-dsl`, and `hive-weave` — **not** on hive-mcp.
Config (`db-path`/`backend`/`store-id`/`writer`) and the base KG **core-norms
classpath resource** are **injected by the host** at store-construction time,
so the backend resolves no env/config of its own.

Reads and writes are bounded and auto-healing via `hive-weave.retry`
(reopen-and-retry on writer-dead failures; timeouts surface immediately).

## Use

```clojure
(require '[hive-datahike.kg.store :as store])

(def s (store/create-store
        {:db-path             "/path/to/db"
         :backend             :file            ; or :mem
         :core-norms-resource "hive_mcp/norms/kg"}))  ; host-injected

(require '[hive-spi.kg.protocol :as kg])
(kg/transact! s [...])
(kg/query s '[:find ?e :where [?e :kg-edge/id _]])
(kg/as-of-db s some-tx)   ; temporal
```

## Layout

```
hive-datahike/
├── deps.edn
├── .hive-project.edn
└── src/hive_datahike/kg/store.clj
```
