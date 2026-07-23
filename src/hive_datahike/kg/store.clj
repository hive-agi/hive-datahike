(ns hive-datahike.kg.store
  "Datahike implementation of hive-spi.kg.protocol/IKGStore + IPersistentKGStore
   + ITemporalKGStore. Config (db-path/backend/store-id/writer) and the core-norms
   classpath resource are injected by the host — the store resolves no env/config
   of its own and carries no hive-mcp dependency."
  (:require [datahike.api :as d]
            [datahike.norm.norm :as norm]
            [datahike.query :as dq]
            [datahike.tools :as tools]
            [hive-spi.kg.protocol :as kg]
            [hive-dsl.result :as r :refer [rescue]]
            [hive-weave.retry :as retry]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log]))
;; SPDX-License-Identifier: MIT

(defonce ^:private addon-norms-registry (atom []))

(def ^:private version-keys
  [:datahike/version
   :hitchhiker.tree/version
   :persistent.set/version
   :konserve/version])

(def ^:private mismatch-type->version-key
  {:db-was-written-with-newer-datahike-version :datahike/version
   :db-was-written-with-newer-hht-version :hitchhiker.tree/version
   :db-was-written-with-newer-pss-version :persistent.set/version
   :db-was-written-with-newer-konserve-version :konserve/version})

(defn runtime-version-provenance
  "Versions loaded by the runtime for Datahike's connect-time checks."
  []
  (select-keys (tools/meta-data) version-keys))

(defn- connection-version-provenance
  [conn]
  {:runtime (runtime-version-provenance)
   :stored (select-keys (:meta (d/db conn)) version-keys)})

(defn- mismatch-version-provenance
  [e]
  (let [{:keys [type stored now] :as data} (ex-data e)]
    (when-let [version-key (get mismatch-type->version-key type)]
      {:runtime (assoc (runtime-version-provenance) version-key now)
       :stored {version-key stored}
       :mismatch (select-keys data [:type :stored :now])})))

(def ^:dynamic *read-timeout-ms*
  "Upper bound for a Datahike read deref. Dynamic so slow cold-cache workloads
   can `binding` a longer ceiling per call site."
  60000)

(def ^:dynamic *write-timeout-ms*
  "Upper bound for a single Datahike transact! deref. Dynamic so large-batch
   writers can `binding` a longer ceiling per call site."
  120000)

(defn register-norms!
  "Register a classpath resource path for addon Datahike norms, applied
   idempotently on ensure-conn!. Call during addon init, before first store access."
  [resource-path]
  (swap! addon-norms-registry conj resource-path)
  (log/info "Registered addon KG norms" {:path resource-path}))

(defn- result-error
  [result]
  (select-keys result
               [:error :message :class :timeout-ms :name
                :version-provenance]))

(defn- throw-read-failed! [label result]
  (throw (ex-info (str "Datahike KG read failed: " label)
                  (assoc (result-error result) :operation label))))

(defn- throw-write-failed! [label result]
  (throw (ex-info (str "Datahike KG write failed: " label)
                  (assoc (result-error result) :operation label))))

(defn- read-with-retry
  "Bounded Datahike read with auto-heal. Per-attempt budget `*read-timeout-ms*`;
   recover via `reopen!` on non-timeout failure; timeouts surface immediately."
  [label reopen! f]
  (retry/with-recovery
    {:timeout-ms *read-timeout-ms*
     :name       label
     :recover!   reopen!
     :on-failure (fn [l result] (throw-read-failed! l result))}
    f))

(defn- write-with-retry
  "Bounded Datahike write with auto-heal. Per-attempt budget `*write-timeout-ms*`;
   recover via `reopen!` on non-timeout failure; timeouts surface immediately."
  [label reopen! f]
  (retry/with-recovery
    {:timeout-ms *write-timeout-ms*
     :name       label
     :recover!   reopen!
     :on-failure (fn [l result] (throw-write-failed! l result))}
    f))

(declare validate-config!)

(defn- validate-config-result
  [cfg]
  (r/try-effect* :datahike/invalid-config
    (validate-config! cfg)
    cfg))

(defn- ensure-database-result
  [cfg]
  (r/let-ok [cfg     (validate-config-result cfg)
             exists? (r/try-effect* :datahike/database-exists-check-failed
                       (d/database-exists? cfg))
             cfg     (if exists?
                       (r/ok cfg)
                       (r/try-effect* :datahike/create-database-failed
                         (log/info "Creating new Datahike database" {:cfg cfg})
                         (d/create-database cfg)
                         cfg))]
    (r/ok cfg)))

(defn- connect-result
  [cfg]
  (try
    (let [conn (d/connect cfg)]
      (when-let [provenance (rescue nil (connection-version-provenance conn))]
        (log/info "Datahike KG version provenance"
                  {:version-provenance provenance}))
      (r/ok conn))
    (catch Exception e
      (let [provenance (rescue nil (mismatch-version-provenance e))]
        (r/err :datahike/connect-failed
               (cond-> {:class (str (class e))
                        :message (.getMessage e)
                        :cause e}
                 provenance
                 (assoc :version-provenance provenance)))))))

(defn- ensure-core-norms-result
  "Apply the host-injected core norms resource, if any. No-op when nil."
  [conn core-norms-resource]
  (if-let [res (some-> core-norms-resource io/resource)]
    (r/try-effect* :datahike/ensure-core-norms-failed
      (log/info "Applying KG norms" {:path core-norms-resource})
      (norm/ensure-norms! conn res)
      conn)
    (r/ok conn)))

(defn- ensure-addon-norms-result
  [conn]
  (r/try-effect* :datahike/ensure-addon-norms-failed
    (doseq [norms-path @addon-norms-registry]
      (when-let [resource (io/resource norms-path)]
        (log/info "Applying addon KG norms" {:path norms-path})
        (norm/ensure-norms! conn resource)))
    conn))

(defn- apply-query-cache-policy!
  "Constrain datahike's global query-result cache at store init. Env
   DATAHIKE_QUERY_CACHE: a positive integer caps retained snapshot buckets; any
   of off/false/none/no/0 (also the default when unset) disables the cache."
  []
  (let [setting (some-> (System/getenv "DATAHIKE_QUERY_CACHE") str/trim str/lower-case)
        size    (some-> setting parse-long)]
    (if (and size (pos? size))
      (do (dq/set-query-cache-size! size)
          (log/info "Datahike query-result cache bounded" {:snapshots size}))
      (do (alter-var-root #'dq/*query-result-cache?* (constantly false))
          (dq/clear-query-cache!)
          (log/info "Datahike query-result cache disabled" {:env (or setting "unset")})))))

(defn- apply-index-ref-type-policy!
  "Constrain the reference strength of datahike's in-memory persistent-sorted-set
   index nodes at store init. Env DATAHIKE_INDEX_REF_TYPE in {strong,soft,weak};
   unset/unrecognized leaves datahike's default (soft). Takes effect on the NEXT
   store open."
  []
  (when-let [choice (some-> (System/getenv "DATAHIKE_INDEX_REF_TYPE")
                            str/trim str/lower-case not-empty)]
    (if (#{"strong" "soft" "weak"} choice)
      (try
        (require 'datahike.index.persistent-set)
        (let [rt-class (Class/forName "org.replikativ.persistent_sorted_set.RefType")
              settings (Class/forName "org.replikativ.persistent_sorted_set.Settings")
              ctor     (.getConstructor settings (into-array Class [Integer/TYPE rt-class]))
              reftype  (some #(when (= choice (str/lower-case (str %))) %)
                             (.getEnumConstants rt-class))
              v        (resolve 'datahike.index.persistent-set/map->settings)]
          (alter-var-root v
                          (constantly
                           (fn [m]
                             (.newInstance ctor (object-array
                                                 [(int (or (:branching-factor m) 0)) reftype])))))
          (log/info "Datahike index ref-type override applied" {:ref-type choice}))
        (catch Throwable e
          (log/warn "Datahike index ref-type override failed; leaving default"
                    {:choice choice :error (.getMessage e)})))
      (log/warn "Unrecognized DATAHIKE_INDEX_REF_TYPE; leaving default (soft)"
                {:value choice}))))

(defn- init-conn-result
  [cfg core-norms-resource]
  (log/info "Initializing Datahike KG store" {:cfg cfg})
  (apply-query-cache-policy!)
  (apply-index-ref-type-policy!)
  (r/let-ok [cfg  (ensure-database-result cfg)
             conn (connect-result cfg)
             conn (ensure-core-norms-result conn core-norms-resource)
             conn (ensure-addon-norms-result conn)]
    (r/ok conn)))

(defn- make-writer-config
  "Build the :writer section of Datahike config. Backends: :self (default),
   :datahike-server {:url :token}, :kabel {:peer-id :local-peer}."
  [writer-opts]
  (case (get writer-opts :backend :self)
    :self            {:backend :self}
    :datahike-server {:backend :datahike-server
                      :url (:url writer-opts)
                      :token (:token writer-opts)}
    :kabel           {:backend :kabel
                      :peer-id (:peer-id writer-opts)
                      :local-peer (:local-peer writer-opts)}
    {:backend :self}))

(defn- coerce-uuid
  "Accept a UUID, a UUID-formatted string, or nil. Return UUID or nil."
  [v]
  (cond
    (uuid? v)   v
    (string? v) (try (java.util.UUID/fromString v) (catch Throwable _ nil))
    :else       nil))

(defn- make-config
  "Create a Datahike configuration map from explicit opts (no env/config.edn
   resolution — that is the host's job).

   opts:
     :db-path    (required for :file backend) on-disk path
     :backend    :file (default) | :mem/:memory
     :id         store id — a UUID, a UUID-string, or nil
     :store-name when :id absent, the store id is nameUUIDFromBytes of this
                 (default \"hive-kg\")
     :index      datahike index (default :datahike.index/persistent-set)
     :writer     distributed write backend opts (see make-writer-config)"
  [& [{:keys [db-path backend index id store-name writer]
       :or {index :datahike.index/persistent-set
            backend :file
            store-name "hive-kg"}}]]
  (let [store-id  (or (coerce-uuid id)
                      (java.util.UUID/nameUUIDFromBytes (.getBytes ^String store-name)))
        store-cfg (case backend
                    :file {:store {:backend :file :path db-path :id store-id}
                           :schema-flexibility :read
                           :index index}
                    (:mem :memory) {:store {:backend :memory :id store-id}
                                    :schema-flexibility :read
                                    :index index}
                    {:store {:backend :file :path db-path :id store-id}
                     :schema-flexibility :read
                     :index index})
        writer-cfg (when writer (make-writer-config writer))]
    (cond-> store-cfg
      writer-cfg (assoc :writer writer-cfg))))

(defn- validate-config!
  "Validate Datahike configuration and create the parent directory if needed."
  [cfg]
  (when-not (map? cfg)
    (throw (ex-info "Datahike config must be a map" {:cfg cfg})))
  (when-not (get-in cfg [:store :backend])
    (throw (ex-info "Datahike config missing :store :backend" {:cfg cfg})))
  (when (= :file (get-in cfg [:store :backend]))
    (let [db-path (get-in cfg [:store :path])]
      (when (or (nil? db-path) (empty? db-path))
        (throw (ex-info "Datahike file backend requires :store :path" {:cfg cfg})))
      (let [dir (io/file db-path)]
        (when-not (.exists (.getParentFile dir))
          (log/info "Creating Datahike parent directory" {:path (.getParent dir)})
          (.mkdirs (.getParentFile dir))))))
  cfg)

(defrecord DatahikeStore [conn-atom cfg core-norms-resource]
  kg/IKGStore

  (ensure-conn! [_this]
    (when (nil? @conn-atom)
      (let [result (init-conn-result cfg core-norms-resource)]
        (when (r/err? result)
          (throw (ex-info "Datahike KG connection initialization failed"
                          (-> result
                              (dissoc :cause)
                              (assoc :cfg cfg))
                          (:cause result))))
        (reset! conn-atom (:ok result))))
    (when (nil? @conn-atom)
      (throw (ex-info "Datahike KG connection is nil after initialization" {:cfg cfg})))
    @conn-atom)

  (transact! [this tx-data]
    ;; d/transact! is async (returns a throwable-promise); deref to block until
    ;; committed. Writer-dead failures reopen the conn and retry once.
    (write-with-retry "transact"
                      #(kg/reset-conn! this)
                      #(deref (d/transact! (kg/ensure-conn! this) tx-data))))

  (query [this q]
    (read-with-retry "query"
                     #(kg/reset-conn! this)
                     #(d/q q (d/db (kg/ensure-conn! this)))))

  (query [this q inputs]
    (read-with-retry "query-with-inputs"
                     #(kg/reset-conn! this)
                     #(apply d/q q (d/db (kg/ensure-conn! this)) inputs)))

  (entity [this eid]
    (read-with-retry "entity"
                     #(kg/reset-conn! this)
                     #(d/entity (d/db (kg/ensure-conn! this)) eid)))

  (entid [this lookup-ref]
    (read-with-retry "entid"
                     #(kg/reset-conn! this)
                     #(let [[attr val] lookup-ref]
                        (d/q '[:find ?e .
                               :in $ ?attr ?val
                               :where [?e ?attr ?val]]
                             (d/db (kg/ensure-conn! this))
                             attr val))))

  (pull-entity [this pattern eid]
    (read-with-retry "pull-entity"
                     #(kg/reset-conn! this)
                     #(d/pull (d/db (kg/ensure-conn! this)) pattern eid)))

  (eids-by-attr [this attr]
    (read-with-retry "eids-by-attr"
                     #(kg/reset-conn! this)
                     #(mapv :e (d/datoms (d/db (kg/ensure-conn! this)) :aevt attr))))

  (db-snapshot [this]
    (read-with-retry "db-snapshot"
                     #(kg/reset-conn! this)
                     #(d/db (kg/ensure-conn! this))))

  (reset-conn! [this]
    ;; NON-DESTRUCTIVE — release conn and reopen against the SAME on-disk DB.
    (log/info "Reopening Datahike KG store (non-destructive)" {:cfg cfg})
    (when-let [c @conn-atom]
      (rescue nil (d/release c)))
    (reset! conn-atom nil)
    (kg/ensure-conn! this))

  (close! [_this]
    (when-let [c @conn-atom]
      (log/info "Closing Datahike KG store" {:cfg cfg})
      (rescue nil (d/release c))
      (reset! conn-atom nil)))

  kg/IPersistentKGStore

  (delete-database! [_this confirm]
    ;; DESTRUCTIVE — guard required. confirm must be :i-mean-it.
    (when-not (= confirm :i-mean-it)
      (throw (ex-info "delete-database! requires confirm=:i-mean-it"
                      {:passed-confirm confirm
                       :hint "This call deletes the database from disk. Pass :i-mean-it explicitly to proceed."
                       :backend :datahike
                       :db-path (get-in cfg [:store :path])})))
    (log/error "[storage/destruction-fired] Datahike delete-database! invoked"
               {:backend :datahike
                :db-path (get-in cfg [:store :path])
                :stacktrace (mapv str (.getStackTrace (Throwable.)))})
    (when-let [c @conn-atom]
      (rescue nil (d/release c)))
    (when (d/database-exists? cfg)
      (d/delete-database cfg))
    (reset! conn-atom nil)
    (log/error "[storage/destruction-completed] Datahike database deleted"
               {:backend :datahike :db-path (get-in cfg [:store :path])})
    nil)

  kg/ITemporalKGStore

  (history-db [this]
    (d/history (d/db (kg/ensure-conn! this))))

  (as-of-db [this tx-or-time]
    (d/as-of (d/db (kg/ensure-conn! this)) tx-or-time))

  (since-db [this tx-or-time]
    (d/since (d/db (kg/ensure-conn! this)) tx-or-time)))

(defn create-store
  "Create a Datahike-backed IKGStore. See `make-config` for config opts;
   `:core-norms-resource` is a classpath path the host injects for the base KG
   norms (nil skips core norms). Returns nil on failure (rescue-wrapped)."
  [& [opts]]
  (rescue nil
          (let [cfg (make-config opts)]
            (log/info "Creating Datahike graph store" {:cfg cfg})
            (->DatahikeStore (atom nil) cfg (:core-norms-resource opts)))))

(defn health
  "Report whether the loaded runtime can open the store and its version provenance."
  [store]
  (try
    {:status :healthy
     :backend :datahike
     :compatible? true
     :version-provenance
     (connection-version-provenance (kg/ensure-conn! store))}
    (catch Exception e
      (let [data (ex-data e)]
        {:status :unhealthy
         :backend :datahike
         :compatible? false
         :error (cond-> {:class (str (class e))
                         :message (.getMessage e)}
                  (:error data) (assoc :type (:error data)))
         :version-provenance
         (or (:version-provenance data)
             (mismatch-version-provenance e)
             {:runtime (rescue {} (runtime-version-provenance))})}))))

(defn history-db
  "Full history database for temporal queries."
  [store]
  (d/history (d/db (kg/ensure-conn! store))))

(defn as-of-db
  "Database as of a specific transaction or timestamp."
  [store tx-or-time]
  (d/as-of (d/db (kg/ensure-conn! store)) tx-or-time))

(defn since-db
  "Database with only facts added since a transaction or timestamp."
  [store tx-or-time]
  (d/since (d/db (kg/ensure-conn! store)) tx-or-time))

(defn query-history
  "Query against the full history database."
  [store q & inputs]
  (if (seq inputs)
    (apply d/q q (history-db store) inputs)
    (d/q q (history-db store))))

(defn query-as-of
  "Query the database as it was at a specific point in time."
  [store tx-or-time q & inputs]
  (if (seq inputs)
    (apply d/q q (as-of-db store tx-or-time) inputs)
    (d/q q (as-of-db store tx-or-time))))
