(ns hive-datahike.store.provider
  "Datahike provider of the hive-contracts storage ports:
   hive-contracts.ports/IDatalogStore, ITemporalStore and ISchemaMigrator.

   `cfg` is a datahike config map, `conn` a datahike connection, db values are
   datahike dbs. Migrations are recorded as datahike.norm records them — one
   `{:tx/norm id}` entity transacted with the migration's tx-data — so a store
   whose norms were applied by datahike.norm.norm/ensure-norms! (ids are the
   norm filename keywords) sees them as already applied."
  (:require [datahike.api :as d]
            [hive-contracts.ports :as p]))
;; SPDX-License-Identifier: MIT

(def norm-attribute
  "Schema of the attribute that marks an applied migration; datahike.norm's own."
  {:db/ident       :tx/norm
   :db/valueType   :db.type/keyword
   :db/cardinality :db.cardinality/one})

(defn- norm-attribute-installed? [db]
  (some? (d/q '[:find ?e . :where [?e :db/ident :tx/norm]] db)))

(defn- ensure-norm-attribute! [conn]
  (when-not (norm-attribute-installed? (d/db conn))
    (d/transact conn {:tx-data [norm-attribute]})))

(defn applied-norms
  "Set of migration ids recorded in `db`."
  [db]
  (if (norm-attribute-installed? db)
    (set (d/q '[:find [?n ...] :where [_ :tx/norm ?n]] db))
    #{}))

(defn migration-tx
  "The tx-data that applies `migration` and records its id."
  [{:keys [id tx-data]}]
  (into [{:tx/norm id}] tx-data))

(defn- run-migrations! [conn migrations]
  (ensure-norm-attribute! conn)
  (reduce (fn [acc {:keys [id] :as migration}]
            (if (contains? (applied-norms (d/db conn)) id)
              (update acc :skipped conj id)
              (do (d/transact conn {:tx-data (migration-tx migration)})
                  (update acc :applied conj id))))
          {:applied [] :skipped []}
          migrations))

(defrecord DatahikeProvider []
  p/IDatalogStore
  (store-exists? [_ cfg] (boolean (d/database-exists? cfg)))
  (create-store! [_ cfg] (d/create-database cfg))
  (delete-store! [_ cfg] (d/delete-database cfg))
  (connect [_ cfg] (d/connect cfg))
  (release! [_ conn] (d/release conn) nil)
  (transact! [_ conn tx-data] (d/transact conn {:tx-data (vec tx-data)}))
  (db [_ conn] (d/db conn))
  (q [_ query db-value inputs] (apply d/q query db-value inputs))
  (pull [_ db-value pattern eid] (d/pull db-value pattern eid))

  p/ITemporalStore
  (basis [_ db-value] (:max-tx db-value))
  (history [_ db-value] (d/history db-value))
  (as-of [_ db-value point] (d/as-of db-value point))
  (since [_ db-value point] (d/since db-value point))

  p/ISchemaMigrator
  (migrate! [_ conn migrations] (run-migrations! conn migrations))
  (applied-migrations [_ conn] (applied-norms (d/db conn))))

(defn provider
  "A provider value satisfying all three storage ports."
  []
  (->DatahikeProvider))
