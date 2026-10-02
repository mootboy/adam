(ns adam.sources.claude-code.runtime
  (:require [adam.knowledge.memory-migration :as memory-migration]
            [adam.memory.service :as memory-service]
            [adam.knowledge.repository :as repository]
            [adam.knowledge.runtime :as knowledge-runtime]
            [adam.knowledge.store :as knowledge-store]
            [adam.replica.identity :as identity]
            [adam.replica.migration :as migration]
            [adam.replica.neo4j :as neo4j]
            [adam.replica.state :as global-state]
            [adam.replica.store :as replica-store]
            [adam.sources.claude-code.reconcile :as reconcile]))

(defn create-runtime
  ([config]
   (create-runtime config {}))
  ([config options]
   (let [runtime-promise (atom nil)
         create-replica (or (:create-replica options)
                            #(neo4j/create-replica config))
         load-user (or (:load-user options)
                       global-state/load-or-create-host!)
         exec! (or (:exec options) knowledge-runtime/node-exec!)
         get-runtime!
         (fn []
           (or @runtime-promise
               (let [created-store (atom nil)
                     promise
                     (-> (js/Promise.resolve nil)
                         (.then (fn [_] (create-replica)))
                         (.then
                          (fn [store]
                            (reset! created-store store)
                            (-> (js/Promise.resolve nil)
                                (.then (fn [_] (load-user)))
                                (.then (fn [user] [store user])))))
                         (.then
                          (fn [[store user]]
                            (let [user-uuid (:user-uuid user)
                                  user-id (identity/user-urn user-uuid)]
                              (-> (replica-store/initialize! store {:id user-id})
                                  (.then
                                   (fn [_]
                                     (if (satisfies?
                                          replica-store/SessionIdentityMigrationStore
                                          store)
                                       (migration/migrate-if-needed!
                                        {:store store :user-id user-id})
                                       (js/Promise.resolve nil))))
                                  (.then
                                   (fn [_]
                                     (if (satisfies?
                                          knowledge-store/MemoryIdentityMigrationStore
                                          store)
                                       (memory-migration/migrate-if-needed!
                                        {:store store :user-id user-id})
                                       (js/Promise.resolve nil))))
                                  (.then
                                   (fn [_]
                                     {:store store
                                      :user-uuid user-uuid}))))))
                         (.catch
                          (fn [error]
                            (reset! runtime-promise nil)
                            (if-let [store @created-store]
                              (-> (replica-store/close! store)
                                  (.catch (fn [_] nil))
                                  (.then (fn [_] (js/Promise.reject error))))
                              (js/Promise.reject error)))))]
                 (reset! runtime-promise promise)
                 promise)))
         process!
         (fn [notification]
           (-> (get-runtime!)
               (.then
                (fn [{:keys [store user-uuid]}]
                  (reconcile/reconcile!
                   {:locator-options (:locator-options options)
                    :notification notification
                    :user-uuid user-uuid
                    :store store
                    :resolve-repository!
                    (fn [cwd resolved-user-uuid]
                      (repository/resolve-git-repository!
                       exec! cwd resolved-user-uuid))})))))
         close!
         (fn []
           (if-let [promise @runtime-promise]
             (-> promise
                 (.then (fn [{:keys [store]}]
                          (replica-store/close! store)))
                 (.catch (fn [_] nil)))
             (js/Promise.resolve nil)))]
     {:process! process!
      :drain-memory! (fn drain-memory!
                       ([] (drain-memory! {}))
                       ([repair-options]
                        (-> (get-runtime!)
                            (.then (fn [runtime]
                                     (memory-service/drain!
                                       (assoc runtime
                                         :inbox-options (:memory-inbox-options options)
                                         :recover-expired? (true? (:recover-expired? repair-options))
                                         :log! #(js/console.error (str "adam memory: " (pr-str %))))))))))
      :close! close!})))
