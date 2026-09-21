;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(ns hive-datahike.kg.history-migrate
  "Copy a Datahike store into a NEW store with `:keep-history? false`.

   Datahike validates the connect-time config against the stored one, so
   `:keep-history?` cannot be changed in place — a store created with history
   refuses to open under a config that disables it. The only route is a copy.

   Contract:
     (plan source-cfg target-cfg)  => the migration plan, no I/O beyond a read
     (migrate! opts)               => Result<report>

   `migrate!` NEVER writes to, deletes, or reconfigures the source. It refuses
   to run without `:i-mean-it true`, and refuses a target that already exists."
  (:require [datahike.api :as d]
            [hive-dsl.result :as r]))

;; =============================================================================
;; Reading the source
;; =============================================================================

(defn schema-entity-ids
  "Entity ids of schema entities — those carrying a `:db/ident`. Their datoms
   are reproduced by transacting the schema, not by copying."
  [db]
  (into #{} (map first) (d/datoms db :aevt :db/ident)))

(defn user-datoms
  "Current-state datoms excluding schema entities, as [e a v] triples."
  [db]
  (let [skip (schema-entity-ids db)]
    (into []
          (comp (remove (fn [[e]] (contains? skip e)))
                (map (fn [[e a v]] [e a v])))
          (d/datoms db :eavt))))

(defn source-census
  "Datom and entity counts of the source, for the before/after comparison."
  [db]
  (let [ds (user-datoms db)]
    {:datoms (count ds)
     :entities (count (into #{} (map first) ds))
     :attributes (count (into #{} (map second) ds))}))

;; =============================================================================
;; Plan
;; =============================================================================

(defn no-history-cfg
  "`cfg` with history disabled. The store path/id must already differ from the
   source's — `migrate!` enforces that."
  [cfg]
  (assoc cfg :keep-history? false))

(defn plan
  "What the migration would do. Pure with respect to the target."
  [source-cfg target-cfg]
  {:source (get-in source-cfg [:store :path])
   :target (get-in target-cfg [:store :path])
   :target-keep-history? (:keep-history? target-cfg)
   :distinct-paths? (not= (get-in source-cfg [:store :path])
                          (get-in target-cfg [:store :path]))})

;; =============================================================================
;; Migrate
;; =============================================================================

(defn- copy-datoms!
  [conn datoms batch-size]
  (reduce (fn [n batch]
            (d/transact conn (mapv (fn [[e a v]] [:db/add e a v]) batch))
            (+ n (count batch)))
          0
          (partition-all batch-size datoms)))

(defn migrate!
  "Copy `:source-cfg`'s current state into `:target-cfg` with history off.

   opts:
     :source-cfg  (required) config of the store to read
     :target-cfg  (required) config of the store to create; must name a
                  DIFFERENT path, and must not already exist
     :schema      (required) the schema tx to install in the target
     :batch-size  datoms per transaction (default 1000)
     :i-mean-it   must be true — this creates a second copy of the store

   Returns Result<{:before {...} :after {...} :copied n :verified? bool}>.
   The source is opened read-only and released untouched."
  [{:keys [source-cfg target-cfg schema batch-size i-mean-it]
    :or   {batch-size 1000}}]
  (let [p (plan source-cfg (no-history-cfg (or target-cfg {})))]
    (cond
      (not (true? i-mean-it))
      (r/err :migrate/refused {:reason :i-mean-it-not-set :plan p})

      (not (:distinct-paths? p))
      (r/err :migrate/refused {:reason :target-path-equals-source :plan p})

      (nil? schema)
      (r/err :migrate/refused {:reason :schema-required :plan p})

      (d/database-exists? target-cfg)
      (r/err :migrate/refused {:reason :target-already-exists :plan p})

      :else
      (r/try-effect*
       :migrate/copy-failed
       (let [tgt-cfg (no-history-cfg target-cfg)
             src     (d/connect source-cfg)
             [before datoms] (try
                               (let [db (d/db src)]
                                 [(source-census db) (user-datoms db)])
                               (finally (d/release src)))]
         (d/create-database tgt-cfg)
         (let [tgt (d/connect tgt-cfg)]
           (try
             (d/transact tgt schema)
             (let [copied (copy-datoms! tgt datoms batch-size)
                   after  (source-census (d/db tgt))]
               {:plan p
                :before before
                :after after
                :copied copied
                :verified? (and (= (:datoms before) (:datoms after))
                                (= (:entities before) (:entities after)))})
             (finally (d/release tgt)))))))))
