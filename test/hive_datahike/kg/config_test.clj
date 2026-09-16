(ns hive-datahike.kg.config-test
  "make-config is a pass-through, not a policy: supplying neither :keep-history?
   nor :value-caps must reproduce the pre-existing config byte for byte."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-datahike.kg.store :as store]))

(def ^:private make-config #'store/make-config)

(def ^:private base-opts
  {:db-path "/tmp/hive-datahike-config-test" :store-name "cfg-test"})

(deftest omitting-the-new-keys-changes-nothing-test
  (testing "neither key appears on the config when the caller supplies neither"
    (let [cfg (make-config base-opts)]
      (is (not (contains? cfg :keep-history?)))
      (is (not (contains? cfg :value-caps)))))
  (testing "the rest of the config is exactly what it always was"
    (let [cfg (make-config base-opts)]
      (is (= :read (:schema-flexibility cfg)))
      (is (= :datahike.index/persistent-set (:index cfg)))
      (is (= :file (get-in cfg [:store :backend])))
      (is (= "/tmp/hive-datahike-config-test" (get-in cfg [:store :path])))
      (is (uuid? (get-in cfg [:store :id]))))))

(deftest keep-history-passes-through-test
  (testing "false is forwarded — and is NOT swallowed as a falsey 'absent'"
    (let [cfg (make-config (assoc base-opts :keep-history? false))]
      (is (contains? cfg :keep-history?))
      (is (false? (:keep-history? cfg)))))
  (testing "true is forwarded"
    (is (true? (:keep-history? (make-config (assoc base-opts :keep-history? true))))))
  (testing "a truthy non-boolean is coerced to a boolean"
    (is (true? (:keep-history? (make-config (assoc base-opts :keep-history? :yes)))))))

(deftest value-caps-passes-through-test
  (testing "the :default keyword is forwarded verbatim"
    (is (= :default (:value-caps (make-config (assoc base-opts :value-caps :default))))))
  (testing "an explicit cap map is forwarded verbatim"
    (let [caps {:max-string-length 4096}]
      (is (= caps (:value-caps (make-config (assoc base-opts :value-caps caps))))))))

(deftest memory-backend-honours-the-new-keys-test
  (testing "the in-memory arm takes the same pass-through"
    (let [cfg (make-config {:backend :memory :store-name "mem-cfg"
                            :keep-history? false :value-caps :default})]
      (is (= :memory (get-in cfg [:store :backend])))
      (is (false? (:keep-history? cfg)))
      (is (= :default (:value-caps cfg))))))

(deftest store-id-is-stable-across-the-new-keys-test
  (testing "adding a policy key must not move the store identity"
    (is (= (get-in (make-config base-opts) [:store :id])
           (get-in (make-config (assoc base-opts :keep-history? false :value-caps :default))
                   [:store :id])))))
