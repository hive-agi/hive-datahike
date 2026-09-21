(ns hive-datahike.kg.history-migrate-test
  "Pins the copy-based route off :keep-history?, and the guards that keep it
   from touching the source."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [hive-dsl.result :as r]
            [hive-datahike.kg.history-migrate :as hm]))

(def ^:private schema
  [{:db/ident :n/id :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity :db/index true}
   {:db/ident :n/parent :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :n/tags :db/valueType :db.type/string :db/cardinality :db.cardinality/many}
   {:db/ident :n/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(defn- cfg [tag & {:keys [keep-history?]}]
  (cond-> {:store {:backend :file
                   :path (str (System/getProperty "java.io.tmpdir")
                              "/dh-hm-" tag "-" (System/nanoTime))
                   :id (random-uuid)}
           :schema-flexibility :write}
    (some? keep-history?) (assoc :keep-history? keep-history?)))

(defn- seeded-source!
  "A source store WITH history: refs, cardinality-many, and a mutation so the
   history segment is genuinely populated."
  []
  (let [c (cfg "src")]
    (d/create-database c)
    (let [conn (d/connect c)]
      (d/transact conn schema)
      (d/transact conn [{:db/id -1 :n/id "root" :n/tags ["a" "b"] :n/n 1}
                        {:n/id "child" :n/parent -1 :n/tags ["c"] :n/n 2}])
      (d/transact conn [{:n/id "root" :n/n 99}])
      (d/release conn))
    c))

;; =============================================================================
;; Guards — every one of these must refuse BEFORE touching anything
;; =============================================================================

(deftest refuses-without-i-mean-it-test
  (let [src (seeded-source!)]
    (let [res (hm/migrate! {:source-cfg src :target-cfg (cfg "t1") :schema schema})]
      (is (r/err? res))
      (is (= :i-mean-it-not-set (:reason res))))))

(deftest refuses-same-path-test
  (let [src (seeded-source!)
        res (hm/migrate! {:source-cfg src :target-cfg src :schema schema :i-mean-it true})]
    (is (r/err? res))
    (is (= :target-path-equals-source (:reason res)))))

(deftest refuses-without-schema-test
  (let [src (seeded-source!)
        res (hm/migrate! {:source-cfg src :target-cfg (cfg "t2") :i-mean-it true})]
    (is (r/err? res))
    (is (= :schema-required (:reason res)))))

(deftest refuses-existing-target-test
  (let [src (seeded-source!)
        tgt (cfg "t3" :keep-history? false)]
    (d/create-database tgt)
    (let [res (hm/migrate! {:source-cfg src :target-cfg tgt :schema schema :i-mean-it true})]
      (is (r/err? res))
      (is (= :target-already-exists (:reason res))))))

;; =============================================================================
;; The copy
;; =============================================================================

(deftest migrate-preserves-state-and-drops-history-test
  (let [src (seeded-source!)
        tgt (cfg "ok")
        res (hm/migrate! {:source-cfg src :target-cfg tgt :schema schema :i-mean-it true})]
    (is (r/ok? res) (pr-str res))
    (let [{:keys [before after copied verified?]} (:ok res)]
      (testing "datom and entity counts survive the copy"
        (is (true? verified?))
        (is (= (:datoms before) (:datoms after)))
        (is (= (:entities before) (:entities after)))
        (is (= (:datoms before) copied)))
      (testing "the target really has history disabled"
        (let [conn (d/connect (hm/no-history-cfg tgt))]
          (try
            (let [db (d/db conn)]
              (testing "current state is intact: refs, cardinality-many, scalars"
                (is (= 2 (d/q '[:find (count ?e) . :where [?e :n/id]] db)))
                (is (= "root" (d/q '[:find ?p . :where [?e :n/id "child"]
                                     [?e :n/parent ?pe] [?pe :n/id ?p]] db)))
                (is (= #{"a" "b"} (set (d/q '[:find [?t ...] :where [?e :n/id "root"]
                                              [?e :n/tags ?t]] db))))
                (testing "the LATEST value of a mutated scalar came across"
                  (is (= 99 (d/q '[:find ?n . :where [?e :n/id "root"] [?e :n/n ?n]] db))))))
            (finally (d/release conn)))))
      (testing "the source is untouched and still readable WITH its history"
        (let [conn (d/connect src)]
          (try
            (is (= 2 (d/q '[:find (count ?e) . :where [?e :n/id]] (d/db conn))))
            (is (some? (d/history (d/db conn))) "source keeps its history segment")
            (finally (d/release conn))))))))

(deftest plan-is-inspectable-before-running-test
  (let [src (cfg "p1") tgt (cfg "p2")
        p   (hm/plan src (hm/no-history-cfg tgt))]
    (is (true? (:distinct-paths? p)))
    (is (false? (:target-keep-history? p)))
    (is (= (get-in src [:store :path]) (:source p)))))
