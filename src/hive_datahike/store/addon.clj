(ns hive-datahike.store.addon
  "IAddon that registers the datahike provider under the hive-contracts
   storage ports :IDatalogStore, :ITemporalStore and :ISchemaMigrator.

   initialize! registers one DatahikeProvider under every port; shutdown!
   unregisters only the ports this addon still owns, so a provider another
   addon installed after it is left alone."
  (:require [hive-addon.protocol :as addon-proto]
            [hive-contracts.registry :as registry]
            [hive-datahike.store.provider :as provider]))
;; SPDX-License-Identifier: MIT

(def ports
  "The storage ports this addon provides."
  [:IDatalogStore :ITemporalStore :ISchemaMigrator])

(def default-addon-id "hive.datahike.store")

(defn register-providers!
  "Register `impl` under every port in `ports`. Returns `impl`."
  [impl]
  (doseq [port ports] (registry/register! port impl))
  impl)

(defn unregister-providers!
  "Unregister each port whose active provider is `impl`. Returns nil."
  [impl]
  (doseq [port ports
          :when (and (registry/registered? port)
                     (identical? impl (registry/provider port)))]
    (registry/unregister! port))
  nil)

(defrecord DatahikeStoreAddon [addon-id impl-atom]
  addon-proto/IAddon
  (addon-id [_] addon-id)
  (addon-type [_] :native)
  (capabilities [_] #{:datalog-store :temporal-store :schema-migrator :health-reporting})
  (initialize! [_ _config]
    (if @impl-atom
      {:success? true :already-initialized? true :errors [] :metadata {:ports ports}}
      (try
        (reset! impl-atom (register-providers! (provider/provider)))
        {:success? true :errors [] :metadata {:ports ports}}
        (catch Exception e
          {:success? false :errors [(ex-message e)] :metadata {:ports ports}}))))
  (shutdown! [_]
    (when-let [impl @impl-atom]
      (unregister-providers! impl)
      (reset! impl-atom nil))
    nil)
  (tools [_] [])
  (schema-extensions [_] {})
  (health [_]
    (if-let [impl @impl-atom]
      {:status  (if (every? #(identical? impl (registry/provider %)) ports) :ok :degraded)
       :details {:ports (into {} (map (fn [p] [p (identical? impl (registry/provider p))])) ports)}}
      {:status :down :details {:reason "not initialized"}}))
  (excluded-tools [_] #{}))

(defn create-addon
  "Create the addon. `config` may carry :addon/id (manifest-injected)."
  ([] (create-addon {}))
  ([config]
   (->DatahikeStoreAddon (or (:addon/id config) default-addon-id) (atom nil))))
