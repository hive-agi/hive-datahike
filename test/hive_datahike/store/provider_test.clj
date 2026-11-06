(ns hive-datahike.store.provider-test
  "The hive-contracts storage conformance suite against the real datahike
   provider, datahike.norm compatibility of migrate!, and the addon's
   registration lifecycle."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike.api :as d]
            [datahike.norm.norm :as norm]
            [hive-addon.protocol :as addon-proto]
            [hive-contracts.ports :as p]
            [hive-contracts.registry :as registry]
            [hive-contracts.store.conformance :as conf]
            [hive-datahike.store.addon :as addon]
            [hive-datahike.store.provider :as provider]))

(use-fixtures :each (fn [t] (registry/reset-providers!) (t) (registry/reset-providers!)))

(defn- mem-cfg []
  {:store {:backend :memory :id (random-uuid)}
   :schema-flexibility :write
   :keep-history? true})

(deftest conformance-suite-passes-on-datahike
  (let [impl    (provider/provider)
        results (conf/run {:store impl :temporal impl :migrator impl :cfg (mem-cfg)})]
    (is (seq results))
    (is (= [] (conf/failures results)))))

(defn- temp-norms-dir []
  (let [dir (.toFile (java.nio.file.Files/createTempDirectory
                      "hive-datahike-norms" (make-array java.nio.file.attribute.FileAttribute 0)))]
    (spit (io/file dir "001-name.edn")
          (pr-str {:tx-data [{:db/ident :person/name :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one}]}))
    dir))

(deftest norms-applied-by-datahike-norm-are-seen-as-applied
  (let [impl (provider/provider)
        cfg  (mem-cfg)
        dir  (temp-norms-dir)]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (norm/ensure-norms! conn dir)
        (testing "the norm filename keyword is the migration id"
          (is (= #{(keyword "001-name.edn")} (p/applied-migrations impl conn))))
        (testing "migrate! skips the norm and applies only the new step"
          (is (= {:applied [:002-age] :skipped [(keyword "001-name.edn")]}
                 (p/migrate! impl conn
                             [{:id (keyword "001-name.edn")
                               :tx-data [{:db/ident :person/name :db/valueType :db.type/string
                                          :db/cardinality :db.cardinality/one}]}
                              {:id :002-age
                               :tx-data [{:db/ident :person/age :db/valueType :db.type/long
                                          :db/cardinality :db.cardinality/one}]}]))))
        (finally
          (d/release conn)
          (d/delete-database cfg)
          (doseq [f (reverse (file-seq dir))] (io/delete-file f true)))))))

(deftest applied-migrations-is-empty-before-any-migration
  (let [impl (provider/provider)
        cfg  (mem-cfg)]
    (p/create-store! impl cfg)
    (let [conn (p/connect impl cfg)]
      (try
        (is (= #{} (p/applied-migrations impl conn)))
        (finally (p/release! impl conn) (p/delete-store! impl cfg))))))

(deftest addon-registers-and-unregisters-every-port
  (let [a (addon/create-addon)]
    (is (every? (complement registry/registered?) addon/ports))
    (is (:success? (addon-proto/initialize! a {})))
    (is (every? registry/registered? addon/ports))
    (is (= :ok (:status (addon-proto/health a))))
    (is (:already-initialized? (addon-proto/initialize! a {})))
    (addon-proto/shutdown! a)
    (is (every? (complement registry/registered?) addon/ports))
    (is (= :down (:status (addon-proto/health a))))))

(deftest addon-shutdown-leaves-a-later-provider-alone
  (let [a     (addon/create-addon)
        other (provider/provider)]
    (addon-proto/initialize! a {})
    (registry/register! :ISchemaMigrator other)
    (addon-proto/shutdown! a)
    (is (identical? other (registry/provider :ISchemaMigrator)))
    (is (not (registry/registered? :IDatalogStore)))))
