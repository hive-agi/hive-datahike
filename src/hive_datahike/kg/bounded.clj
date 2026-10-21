(ns hive-datahike.kg.bounded
  "Bounded Datahike calls: every read runs on ONE bounded executor under a
   deadline, with one reopen-and-retry on a non-timeout failure.

   Why not `hive-weave.retry/with-recovery`: up to hive-weave 0.4.2 it calls
   `safe-future-call` without a :pool, so every read is a `clojure.core/future`
   on the unbounded send-off pool (measured: 114 send-off threads, the largest
   CPU bucket in the JVM). `safe-future-call` itself takes :pool since 0.4.x,
   which is all this namespace needs. Once a hive-weave release whose
   `with-recovery` forwards :pool is pinned in deps.edn, `read-with-retry` can
   collapse onto it with the same options.

   The executor is a port: the store is built with one (`:read-pool` on
   `create-store`) and tests hand it a recording one."
  (:require [hive-dsl.result :as r]
            [hive-weave.pool :as wpool]
            [hive-weave.retry :as retry]
            [hive-weave.safe :as safe]
            [taoensso.timbre :as log]))
;; SPDX-License-Identifier: MIT

(def default-read-pool-size
  "Concurrent Datahike reads the shared pool runs. Reads are CPU and index-cache
   bound, so more threads than this buy contention, not throughput."
  8)

(def default-read-queue-capacity
  "Reads queued behind the running ones before a read is shed as
   :weave/rejected."
  1024)

(defonce ^:private shared-read-pool
  (delay (wpool/make-pool {:name           "hive-datahike-read"
                           :size           default-read-pool-size
                           :queue-capacity default-read-queue-capacity
                           :rejection      :abort})))

(defn default-read-pool
  "The process-wide read pool, created on first use. Process-wide on purpose:
   the bound has to hold across stores and callers. :abort so a saturated pool
   sheds a read as an error instead of running it on the caller's thread, where
   the deadline cannot reach it. Its threads are daemons; it is never shut down."
  []
  @shared-read-pool)

(defn read-pool-of
  "The pool `store` reads on: its own :read-pool when it was built with one,
   else the process-wide default."
  [store]
  (or (:read-pool store) (default-read-pool)))

(defn- result-error
  [result]
  (select-keys result
               [:error :message :class :timeout-ms :name
                :version-provenance]))

(defn throw-read-failed! [label result]
  (throw (ex-info (str "Datahike KG read failed: " label)
                  (assoc (result-error result) :operation label))))

(defn throw-write-failed! [label result]
  (throw (ex-info (str "Datahike KG write failed: " label)
                  (assoc (result-error result) :operation label))))

(defn- retryable?
  "A failure a reopen can cure. Not a timeout (alive but slow: a second attempt
   only doubles the wait) and not a rejection (the pool is shedding load:
   reopening the connection would punish every other reader for it)."
  [result]
  (and (r/err? result)
       (not (#{:weave/timeout :weave/rejected} (:error result)))))

(defn read-with-retry
  "Run read thunk `f` as ONE task on `pool` under `timeout-ms`. On a retryable
   failure call `recover!` once (on the caller's thread) and run `f` once more,
   again as one task. Returns the value or throws `throw-read-failed!`.

   Options: :timeout-ms (pos-int, required), :pool (ExecutorService, required),
   :name (label), :recover! (0-arg fn)."
  [{:keys [timeout-ms pool name recover!]} f]
  {:pre [(pos-int? timeout-ms) (some? pool)]}
  (let [attempt (fn [label] (safe/safe-future-call
                             {:timeout-ms timeout-ms :name label :pool pool}
                             f))
        first-r (attempt name)]
    (cond
      (r/ok? first-r)            (:ok first-r)
      (not (retryable? first-r)) (throw-read-failed! name first-r)
      :else
      (do (log/warn "Datahike read failed, reopening and retrying once"
                    {:operation name :error (:error first-r)})
          (when recover! (recover!))
          (let [retry-r (attempt (str name "/retry"))]
            (if (r/ok? retry-r)
              (:ok retry-r)
              (throw-read-failed! name retry-r)))))))

(defn write-with-retry
  "Bounded Datahike write with auto-heal. Per-attempt budget `timeout-ms`;
   recover via `recover!` on non-timeout failure; timeouts surface immediately."
  [{:keys [timeout-ms name recover!]} f]
  (retry/with-recovery
    {:timeout-ms timeout-ms
     :name       name
     :recover!   recover!
     :on-failure (fn [l result] (throw-write-failed! l result))}
    f))
