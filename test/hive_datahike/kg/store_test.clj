(ns hive-datahike.kg.store-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [hive-datahike.kg.store :as store]
            [hive-spi.kg.protocol :as kg]))

(defn- memory-store []
  (store/create-store
   {:backend :memory
    :store-name (str "hive-datahike-test-" (random-uuid))}))

(deftest runtime-version-provenance-test
  (testing "the loaded runtime reports every version Datahike checks at connect time"
    (let [provenance (store/runtime-version-provenance)]
      (is (string? (:datahike/version provenance)))
      (is (string? (:persistent.set/version provenance)))
      (is (string? (:konserve/version provenance)))
      (is (contains? provenance :hitchhiker.tree/version)))))

(deftest health-reports-compatible-runtime-and-store-test
  (let [sut (memory-store)]
    (try
      (let [result (store/health sut)]
        (is (= :healthy (:status result)))
        (is (= :datahike (:backend result)))
        (is (true? (:compatible? result)))
        (is (= (store/runtime-version-provenance)
               (get-in result [:version-provenance :runtime])))
        (is (string? (get-in result
                             [:version-provenance :stored :konserve/version]))))
      (finally
        (kg/close! sut)))))

(deftest connect-mismatch-preserves-version-provenance-test
  (let [mismatch {:type :db-was-written-with-newer-konserve-version
                  :stored "0.9.353"
                  :now "0.9.352"}]
    (with-redefs [d/database-exists? (constantly true)
                  d/connect (fn [_]
                              (throw
                               (ex-info
                                "Database was written with newer konserve version."
                                mismatch)))]
      (let [sut (memory-store)]
        (try
          (kg/ensure-conn! sut)
          (is false "connect must reject a store written by a newer runtime")
          (catch clojure.lang.ExceptionInfo e
            (let [data       (ex-data e)
                  provenance (:version-provenance data)]
              (is (= :datahike/connect-failed (:error data)))
              (is (= mismatch (:mismatch provenance)))
              (is (= "0.9.353"
                     (get-in provenance [:stored :konserve/version])))
              (is (= "0.9.352"
                     (get-in provenance [:runtime :konserve/version])))
              (is (some? (.getCause e))))))))))

(deftest temporal-queries-run-through-the-port
  (let [sut (memory-store)]
    (try
      (let [report (kg/transact! sut [{:db/id -1 :name "alpha"}])
            tx     (get-in report [:db-after :max-tx])
            q      '[:find ?n :where [_ :name ?n]]]
        (testing "query-as-of reads the db at a transaction without exposing it"
          (is (= #{["alpha"]} (kg/query-as-of sut tx q))))
        (testing "query-history sees the asserted fact"
          (is (contains? (kg/query-history sut q) ["alpha"])))
        (testing "the inputs arity binds extra :in parameters"
          (is (= #{["alpha"]}
                 (kg/query-as-of sut tx
                                 '[:find ?n :in $ ?want :where [_ :name ?n] [(= ?n ?want)]]
                                 ["alpha"])))
          (is (contains? (kg/query-history
                          sut
                          '[:find ?n :in $ ?want :where [_ :name ?n] [(= ?n ?want)]]
                          ["alpha"])
                         ["alpha"]))))
      (finally
        (kg/close! sut)))))
