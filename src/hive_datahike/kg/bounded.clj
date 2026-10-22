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

(def ^:dynamic *cancel*
  "The running read's cancel token, bound on the pool thread for the duration of
   one attempt; nil outside a read. An IDeref whose value is nil while the read
   may go on and a cancellation map once it must stop: its caller gave up
   (interrupted or timed out), the pool thread was interrupted, or the attempt's
   deadline passed. Datahike's d/q takes it as :cancel; loops the store runs
   itself call `check-cancel!`."
  nil)

(defn- cancellation?
  "Whether `t` is a read stopping because it was cancelled, rather than failing."
  [t]
  (let [d (ex-data t)]
    (boolean (or (:datahike/canceled d)
                 (= :datahike/query-timeout (:type d))
                 (instance? InterruptedException t)))))

(defn check-cancel!
  "Throw if the running read has been cancelled. A no-op outside a read."
  []
  (when-let [c *cancel*]
    (when-let [v @c]
      (throw (ex-info "read canceled" v)))))

(defn with-cancel
  "Datahike query `q` carrying the running read's cancel token as :cancel, so
   d/q stops at its next check point once the read is cancelled. A query map
   (one with :query) keeps every key, and a :cancel of its own still applies; any
   other query form becomes {:query q}. Outside a read, `q` unchanged."
  [q]
  (if-let [token *cancel*]
    (if (and (map? q) (contains? q :query))
      (assoc q :cancel (if-let [own (:cancel q)]
                         (reify clojure.lang.IDeref
                           (deref [_] (or @own @token)))
                         token))
      {:query q :cancel token})
    q))

(defn- cancel-token
  "A cancel token for one attempt: set through `cancelled`, by an interrupt of
   the thread that reads it, or by `deadline-ns` (System/nanoTime) passing."
  [cancelled timeout-ms deadline-ns]
  (reify clojure.lang.IDeref
    (deref [_]
      (or @cancelled
          (when (.isInterrupted (Thread/currentThread))
            {:datahike/canceled true :reason :interrupted})
          (when (>= (- (System/nanoTime) (long deadline-ns)) 0)
            {:type :datahike/query-timeout :timeout-ms timeout-ms})))))

(defn- attempt!
  "Run `f` as ONE task on `pool` under `timeout-ms`; returns a Result. Whenever
   the caller stops waiting (deadline, interrupt, any throw) the pool task is
   cancelled too: its token flips, so d/q and `check-cancel!` stop the work, and
   its Future is cancelled with interrupt, so a read still queued never starts."
  [{:keys [timeout-ms name pool]} f]
  (let [cancelled (volatile! nil)
        deadline  (+ (System/nanoTime) (* 1000000 (long timeout-ms)))
        token     (cancel-token cancelled timeout-ms deadline)
        task      (fn []
                    (binding [*cancel* token]
                      (try (f) (catch Throwable t {::exception t}))))
        fut       (try
                    (wpool/submit! pool task)
                    (catch RuntimeException e
                      (if (wpool/rejected? e) ::rejected (throw e))))]
    (if (= ::rejected fut)
      (do (log/warn "Datahike read rejected: pool saturated" {:operation name})
          (r/err :weave/rejected {:name name :pool (wpool/pool-stats pool)}))
      (let [done? (volatile! false)]
        (try
          (let [result (deref fut timeout-ms ::timed-out)
                ex     (when (map? result) (::exception result))]
            (cond
              (= ::timed-out result)
              (do (log/warn "Datahike read timed out" {:operation name :timeout-ms timeout-ms})
                  (r/err :weave/timeout {:name name :timeout-ms timeout-ms}))

              (and ex (cancellation? ex))
              (do (vreset! done? true)
                  (log/warn "Datahike read timed out" {:operation name :timeout-ms timeout-ms})
                  (r/err :weave/timeout {:name name :timeout-ms timeout-ms}))

              ex
              (do (vreset! done? true)
                  (r/err :weave/exception {:name    name
                                           :message (.getMessage ^Throwable ex)
                                           :class   (str (class ex))}))

              :else
              (do (vreset! done? true)
                  (r/ok result))))
          (finally
            (when-not @done?
              (vreset! cancelled {:datahike/canceled true :reason :abandoned})
              (future-cancel fut))))))))

(defn read-with-retry
  "Run read thunk `f` as ONE task on `pool` under `timeout-ms`. On a retryable
   failure call `recover!` once (on the caller's thread) and run `f` once more,
   again as one task. Returns the value or throws `throw-read-failed!`.

   A read the caller abandons (timeout, or the caller's thread interrupted, as a
   future-cancel of the caller does) is cancelled on the pool as well; inside
   `f`, `*cancel*` is that read's token (see `with-cancel`, `check-cancel!`).

   Options: :timeout-ms (pos-int, required), :pool (ExecutorService, required),
   :name (label), :recover! (0-arg fn)."
  [{:keys [timeout-ms pool name recover!]} f]
  {:pre [(pos-int? timeout-ms) (some? pool)]}
  (let [attempt (fn [label] (attempt! {:timeout-ms timeout-ms :name label :pool pool} f))
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
