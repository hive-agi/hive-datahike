(ns hive-datahike.kg.bounded-test
  "Reads run on the store's bounded pool, and a batch pull is ONE task.

   The pool is a port: the store is built with a recording ExecutorService
   that counts what it is handed and runs it on the submitting thread, so the
   counts are exact and nothing waits on a clock."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-datahike.kg.bounded :as bounded]
            [hive-datahike.kg.store :as store]
            [hive-spi.kg.protocol :as kg])
  (:import [java.util.concurrent AbstractExecutorService RejectedExecutionException]))

(defn- recording-pool
  "ExecutorService port. Runs each task inline and counts it in `submitted`.
   With `reject?` it refuses every task, as a saturated :abort pool does."
  ([submitted] (recording-pool submitted false))
  ([submitted reject?]
   (proxy [AbstractExecutorService] []
     (execute [^Runnable task]
       (when reject? (throw (RejectedExecutionException. "saturated")))
       (swap! submitted inc)
       (.run task))
     (shutdown [])
     (shutdownNow [] [])
     (isShutdown [] false)
     (isTerminated [] false)
     (awaitTermination [_ _] true))))

(defn- store-on [pool]
  (store/create-store {:backend    :memory
                       :store-name (str "hive-datahike-bounded-" (random-uuid))
                       :read-pool  pool}))

(defn- seed! [sut n]
  (let [report (kg/transact! sut (mapv (fn [i] {:db/id (- (inc i)) :name (str "n" i)})
                                       (range n)))]
    (vec (sort (vals (dissoc (:tempids report) :db/current-tx))))))

(deftest pull-many-submits-one-task-for-a-batch
  (let [submitted (atom 0)
        sut       (store-on (recording-pool submitted))]
    (try
      (let [eids (seed! sut 25)]
        (testing "a batch of N pulls is ONE task on the pool"
          (reset! submitted 0)
          (let [rows (store/pull-many sut '[:name] eids)]
            (is (= 1 @submitted))
            (is (= (mapv #(str "n" %) (range 25)) (mapv :name rows)))))
        (testing "N single pulls are N tasks: the cost the batch removes"
          (reset! submitted 0)
          (doseq [e eids] (kg/pull-entity sut '[:name] e))
          (is (= 25 @submitted)))
        (testing "an empty batch submits nothing"
          (reset! submitted 0)
          (is (= [] (store/pull-many sut '[:name] [])))
          (is (= 0 @submitted))))
      (finally (kg/close! sut)))))

(deftest every-read-runs-on-the-store-pool
  (let [submitted (atom 0)
        sut       (store-on (recording-pool submitted))]
    (try
      (seed! sut 3)
      (reset! submitted 0)
      (kg/query sut '[:find ?n :where [_ :name ?n]])
      (kg/eids-by-attr sut :name)
      (kg/db-snapshot sut)
      (is (= 3 @submitted) "one pool task per read, none on the unbounded future pool")
      (finally (kg/close! sut)))))

(deftest a-saturated-pool-fails-the-read-without-reopening
  (let [submitted (atom 0)
        recovers  (atom 0)]
    (testing "a rejection throws a read failure and never calls recover!"
      (let [e (try
                (bounded/read-with-retry {:timeout-ms 1000
                                          :pool       (recording-pool submitted true)
                                          :name       "probe"
                                          :recover!   #(swap! recovers inc)}
                                         (constantly :never))
                nil
                (catch clojure.lang.ExceptionInfo e e))]
        (is (some? e))
        (is (= :weave/rejected (:error (ex-data e))))
        (is (= "probe" (:operation (ex-data e))))
        (is (= 0 @recovers))))))

(deftest a-failed-read-reopens-and-retries-once
  (let [submitted (atom 0)
        recovers  (atom 0)
        calls     (atom 0)
        v         (bounded/read-with-retry {:timeout-ms 1000
                                            :pool       (recording-pool submitted)
                                            :name       "flaky"
                                            :recover!   #(swap! recovers inc)}
                                           #(if (= 1 (swap! calls inc))
                                              (throw (ex-info "writer dead" {}))
                                              :healed))]
    (is (= :healed v))
    (is (= 1 @recovers))
    (is (= 2 @submitted) "both attempts ran on the pool")))

(deftest the-default-pool-is-bounded-and-shared
  (let [p (bounded/default-read-pool)]
    (is (identical? p (bounded/default-read-pool)))
    (is (= bounded/default-read-pool-size (.getMaximumPoolSize ^java.util.concurrent.ThreadPoolExecutor p)))
    (is (identical? p (bounded/read-pool-of {})))))
