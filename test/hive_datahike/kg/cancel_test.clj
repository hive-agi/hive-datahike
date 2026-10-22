(ns hive-datahike.kg.cancel-test
  "A read the caller abandons stops on the pool too: the pool thread is free
   within a bound and the pool is left with no busy threads."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-datahike.kg.bounded :as bounded]
            [hive-datahike.kg.store :as store]
            [hive-spi.kg.protocol :as kg]
            [hive-weave.pool :as wpool])
  (:import [java.util.concurrent ThreadPoolExecutor]))

(def ^:private free-bound-ms 500)

(def ^:private slow-q
  '[:find [(pull ?e [*]) ...] :where [?e :name _]])

(defn- seed! [sut n]
  (doseq [chunk (partition-all 5000 (range n))]
    (kg/transact! sut (mapv (fn [i] {:db/id (- (inc i)) :name (str "n" i)}) chunk))))

(defn- active [^ThreadPoolExecutor p] (.getActiveCount p))

(defn- await-idle-ms
  "Milliseconds until `p` has no active thread, or nil past `limit-ms`."
  [p limit-ms]
  (let [t0 (System/nanoTime)]
    (loop []
      (let [el (/ (- (System/nanoTime) t0) 1e6)]
        (cond (zero? (active p)) el
              (> el limit-ms)    nil
              :else              (do (Thread/sleep 5) (recur)))))))

(defn- await-busy [p]
  (loop [i 0]
    (when (and (zero? (active p)) (< i 400))
      (Thread/sleep 5)
      (recur (inc i)))))

(defn- with-slow-store [f]
  (let [pool (wpool/make-pool {:name "hive-datahike-read-cancel-test" :size 2
                               :queue-capacity 16 :rejection :abort})
        sut  (store/create-store {:backend    :memory
                                  :store-name (str "hive-datahike-cancel-" (random-uuid))
                                  :read-pool  pool})]
    (try
      (seed! sut 60000)
      (f sut pool)
      (finally
        (kg/close! sut)
        (.shutdownNow ^ThreadPoolExecutor pool)))))

(deftest a-cancelled-caller-frees-its-pool-thread
  (with-slow-store
    (fn [sut pool]
      (testing "the query is slow enough that a cancel lands mid-read"
        (let [t0 (System/nanoTime)]
          (kg/query sut slow-q)
          (is (> (/ (- (System/nanoTime) t0) 1e6) 300))))
      (testing "future-cancel of the caller stops the pooled query"
        (let [caller (future (kg/query sut slow-q))]
          (await-busy pool)
          (is (= 1 (active pool)) "the read is running on the pool")
          (future-cancel caller)
          (let [ms (await-idle-ms pool free-bound-ms)]
            (is (some? ms) (str "pool thread still busy " free-bound-ms " ms after cancel"))
            (is (zero? (active pool)) "no stuck threads"))))
      (testing "the pool still serves reads afterwards, with the same answer"
        (is (= 60000 (count (kg/query sut slow-q))))
        (is (= 60000 (count (kg/eids-by-attr sut :name))))))))

(deftest a-timed-out-read-frees-its-pool-thread
  (with-slow-store
    (fn [sut pool]
      (let [e (binding [store/*read-timeout-ms* 50]
                (try (kg/query sut slow-q) nil
                     (catch clojure.lang.ExceptionInfo e e)))]
        (is (= :weave/timeout (:error (ex-data e))))
        (is (some? (await-idle-ms pool free-bound-ms)))
        (is (zero? (active pool)))))))

(deftest a-cancelled-batch-pull-frees-its-pool-thread
  (with-slow-store
    (fn [sut pool]
      (let [eids   (kg/eids-by-attr sut :name)
            caller (future (dotimes [_ 20] (store/pull-many sut '[*] eids)))]
        (await-busy pool)
        (future-cancel caller)
        (is (some? (await-idle-ms pool free-bound-ms)))
        (is (zero? (active pool)))))))

(deftest with-cancel-keeps-the-query
  (testing "outside a read the query is untouched"
    (is (= '[:find ?e] (bounded/with-cancel '[:find ?e]))))
  (testing "inside a read a vector query becomes a query map with the token"
    (binding [bounded/*cancel* (volatile! nil)]
      (let [m (bounded/with-cancel '[:find ?e])]
        (is (= '[:find ?e] (:query m)))
        (is (some? (:cancel m))))
      (let [m (bounded/with-cancel {:query '[:find ?e] :limit 3})]
        (is (= 3 (:limit m)))
        (is (some? (:cancel m)))))))
