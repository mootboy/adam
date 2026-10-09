(ns adam.replica.register
  (:require [adam.knowledge.index :as knowledge-index]
            [adam.knowledge.memory-migration :as memory-migration]
            [adam.knowledge.migration :as knowledge-migration]
            [adam.knowledge.repository :as knowledge-repository]
            [adam.knowledge.store :as knowledge-store]
            [adam.knowledge.surfaces :as knowledge-surfaces]
            [adam.memory.service :as memory-service]
            [adam.memory.inbox :as memory-inbox]
            [adam.sources.claude-code.inbox :as transcript-inbox]
            [adam.sources.claude-code.worker :as notification-worker]
            [adam.sources.claude-code.reconcile :as claude-reconcile]
            [adam.replica.commands :as commands]
            [adam.replica.config :as config]
            [adam.replica.identity :as identity]
            [adam.replica.migration :as replica-migration]
            [adam.replica.neo4j :as neo4j]
            [adam.replica.state :as global-state]
            [adam.replica.store :as store]
            [adam.replica.sync :as sync]
            [clojure.string :as string]))

(defn- error-message [error]
  (or (.-message error) (str error)))

(defn- invoke [object method & arguments]
  (.apply (aget object method) object (to-array arguments)))

(defn- notify! [ctx message level]
  (when-let [ui (aget ctx "ui")]
    (invoke ui "notify" message level)))

(defn- session-file [ctx]
  (when-let [manager (aget ctx "sessionManager")]
    (invoke manager "getSessionFile")))

(defn- current-leaf-id [ctx]
  (when-let [manager (aget ctx "sessionManager")]
    (when-let [get-leaf-id (aget manager "getLeafId")]
      (.call get-leaf-id manager))))

(defn- mask-email [email]
  (let [separator (string/index-of email "@")]
    (if (and separator (pos? separator))
      (str (subs email 0 1) "***" (subs email separator))
      "***")))

(defn- format-ms [value]
  (str (/ (.round js/Math (* value 10)) 10) " ms"))

(defn- render-status [runtime-state]
  (let [{:keys [configured? connected? reason user-uuid git-email active-session-file
                last-result last-mirrored-at last-error file-evidence-indexed-at
                file-evidence-repository file-evidence-status file-evidence-error
                memory-adapter-status session-identity-version
                session-identity-migration-status session-identity-migration-error
                memory-identity-version memory-identity-migration-status
                memory-identity-migration-error
                code-memory-version code-memory-rebuild-status
                code-memory-rebuild-error last-timing memory-notifications memory-notifications-error]} runtime-state]
    (string/join
     "\n"
     (remove nil?
             [(str "adam session replica: "
                   (cond
                     (not configured?) "disabled"
                     connected? "connected"
                     :else "not connected"))
              (when reason (str "Reason: " reason))
              (when user-uuid (str "User: " user-uuid))
              (when git-email (str "Git identity: " (mask-email git-email)))
              (when active-session-file (str "Session: " active-session-file))
              (when last-result (str "Last sync: " (name (:status last-result))))
              (when last-mirrored-at (str "Mirrored at: " last-mirrored-at))
              (when last-error (str "Last error: " last-error))
              (when memory-notifications
                (str "Memory notifications: " (:acknowledged memory-notifications) " acknowledged, "
                     (:pending memory-notifications) " pending, " (:failed memory-notifications) " failed; "
                     (:source-expired memory-notifications) " source notifications expired/parked; "
                     (:recovered memory-notifications) " parked locators recovered; "
                     (when (contains? memory-notifications :parked-pending)
                       (str (:parked-pending memory-notifications) " parked remaining at explicit repair; "))
                     (:records-written memory-notifications) " records appended; "
                     (:streams-synchronized memory-notifications) " streams, "
                     (:conflicts memory-notifications) " conflicts, "
                     (:unresolved-citations memory-notifications) " unresolved citations; "
                     (format-ms (:duration-ms memory-notifications))))
              (when memory-notifications-error (str "Memory reconciliation: " memory-notifications-error))
              (when last-timing
                (str "Last timing: " (name (:event last-timing))
                     " completed " (:completed-at last-timing)
                     ", total " (format-ms (:total-ms last-timing))
                     " (queue " (format-ms (:queue-wait-ms last-timing))
                     ", initialization " (format-ms (:initialization-ms last-timing))
                     ", replica " (format-ms (:replica-sync-ms last-timing))
                     ", repository " (format-ms (:repository-discovery-ms last-timing))
                     ", extraction " (format-ms (:evidence-extraction-ms last-timing))
                     ", projection " (format-ms (:neo4j-projection-ms last-timing)) ")"))
              (when file-evidence-indexed-at
                (str "File evidence indexed: " file-evidence-indexed-at))
              (when file-evidence-repository
                (str "File evidence repository: " file-evidence-repository))
              (when file-evidence-status (str "File evidence: " file-evidence-status))
              (when file-evidence-error (str "File evidence error: " file-evidence-error))
              (when session-identity-version
                (str "Session identity schema: v" session-identity-version))
              (when session-identity-migration-status
                (str "Session identity migration: " session-identity-migration-status))
              (when session-identity-migration-error
                (str "Session identity migration error: " session-identity-migration-error))
              (when memory-identity-version
                (str "Memory identity schema: v" memory-identity-version))
              (when memory-identity-migration-status
                (str "Memory identity migration: " memory-identity-migration-status))
              (when memory-identity-migration-error
                (str "Memory identity migration error: " memory-identity-migration-error))
              (when code-memory-version
                (str "Code memory schema: v" code-memory-version))
              (when code-memory-rebuild-status
                (str "Code memory rebuild: " code-memory-rebuild-status))
              (when code-memory-rebuild-error
                (str "Code memory rebuild error: " code-memory-rebuild-error))
              (when memory-adapter-status
                (str "Memory adapter: " memory-adapter-status))]))))

(defn register!
  ([pi]
   (register! pi {}))
  ([pi options]
   (let [resolved-config (or (:config options)
                             (config/resolve-process-config))
         now-ms (or (:now-ms options) #(.now js/performance))
         record-duration!
         (fn [timing key started-at]
           (swap! timing assoc key (max 0 (- (now-ms) started-at))))
         runtime-state (atom {:configured? (:enabled? resolved-config)
                              :connected? false
                              :reason (when-not (:enabled? resolved-config)
                                        (:reason resolved-config))})
         replica-promise (atom nil)
         user-promise (atom nil)
         queue (atom (js/Promise.resolve nil))
         initialized-identities (atom #{})
         file-evidence-schema-promise (atom nil)
         warned-unavailable? (atom false)
         create-replica (or (:create-replica options)
                            #(neo4j/create-replica resolved-config))
         load-user (or (:load-user options)
                       global-state/load-or-create-host!)
         resolve-git-identity!
         (or (:resolve-git-identity options)
             (fn [ctx]
               (if-let [cwd (aget ctx "cwd")]
                 (-> (invoke pi
                             "exec"
                             "git"
                             #js ["config" "--get" "user.email"]
                             #js {:cwd cwd :timeout 2000})
                     (.then
                      (fn [result]
                        (when (zero? (aget result "code"))
                          (identity/git-email-identity (aget result "stdout")))))
                     (.catch (fn [_] nil)))
                 (js/Promise.resolve nil))))
         get-user!
         (fn []
           (or @user-promise
               (let [promise
                     (-> (js/Promise.resolve nil)
                         (.then (fn [_] (load-user)))
                         (.then
                          (fn [user]
                            (swap! runtime-state assoc :user-uuid (:user-uuid user))
                            user)))]
                 (reset! user-promise promise)
                 promise)))
         resolve-repository!
         (or (:resolve-repository options)
             (fn [cwd user-uuid]
               (knowledge-repository/resolve-git-repository!
                (fn [command arguments exec-options]
                  (invoke pi "exec" command arguments exec-options))
                cwd
                user-uuid)))
         get-replica!
         (fn []
           (or @replica-promise
               (let [promise
                     (-> (js/Promise.resolve nil)
                         (.then (fn [_] (create-replica)))
                         (.then
                          (fn [replica]
                            (-> (get-user!)
                                (.then
                                 (fn [user]
                                   (let [user-id (identity/user-urn (:user-uuid user))]
                                     (-> (store/initialize! replica {:id user-id})
                                         (.then
                                          (fn [_]
                                            (if (satisfies?
                                                 store/SessionIdentityMigrationStore
                                                 replica)
                                              (-> (replica-migration/migrate-if-needed!
                                                   {:store replica :user-id user-id})
                                                  (.catch
                                                   (fn [error]
                                                     (swap! runtime-state assoc
                                                            :session-identity-migration-status
                                                            "waiting to retry"
                                                            :session-identity-migration-error
                                                            (error-message error))
                                                     (js/Promise.reject error))))
                                              (js/Promise.resolve
                                               {:status :unsupported
                                                :version nil}))))
                                         (.then
                                          (fn [result]
                                            (swap! runtime-state assoc
                                                   :session-identity-version (:version result)
                                                   :session-identity-migration-status
                                                   (name (:status result))
                                                   :session-identity-migration-error nil)
                                            (if (satisfies?
                                                 knowledge-store/MemoryIdentityMigrationStore
                                                 replica)
                                              (-> (memory-migration/migrate-if-needed!
                                                   {:store replica :user-id user-id})
                                                  (.catch
                                                   (fn [error]
                                                     (swap! runtime-state assoc
                                                            :memory-identity-migration-status
                                                            "waiting to retry"
                                                            :memory-identity-migration-error
                                                            (error-message error))
                                                     (js/Promise.reject error))))
                                              (js/Promise.resolve
                                               {:status :unsupported
                                                :version nil}))))
                                         (.then
                                          (fn [result]
                                            (swap! runtime-state assoc
                                                   :memory-identity-version (:version result)
                                                   :memory-identity-migration-status
                                                   (name (:status result))
                                                   :memory-identity-migration-error nil)
                                            replica)))))))))
                         (.catch
                          (fn [error]
                            (reset! replica-promise nil)
                            (js/Promise.reject error))))]
                 (reset! replica-promise promise)
                 promise)))
         project-file-evidence!
         (fn [replica path user-uuid selected-leaf-id selection-known? timing]
           (if-not (satisfies? knowledge-store/FileEvidenceStore replica)
             (js/Promise.resolve nil)
             (let [schema-started-at (now-ms)
                   projection-input
                   (cond-> {:store replica
                            :path path
                            :user-uuid user-uuid
                            :resolve-repository #(resolve-repository! % user-uuid)
                            :now-ms now-ms
                            :on-timing
                            (fn [index-timing]
                              (swap! timing update :repository-discovery-ms +
                                     (:repository-discovery-ms index-timing))
                              (swap! timing update :evidence-extraction-ms +
                                     (:evidence-extraction-ms index-timing))
                              (swap! timing update :neo4j-projection-ms +
                                     (:neo4j-projection-ms index-timing)))
                            :on-memory-diagnostics
                            (fn [diagnostics]
                              (swap! runtime-state assoc
                                     :memory-adapter-status
                                     (when (seq diagnostics)
                                       (str (count diagnostics)
                                            " producer entr"
                                            (if (= 1 (count diagnostics)) "y" "ies")
                                            " ignored"))))}
                     selection-known? (assoc :current-leaf-id selected-leaf-id))]
               (-> (or @file-evidence-schema-promise
                       (let [promise
                             (-> (knowledge-store/ensure-file-evidence-schema! replica)
                                 (.catch
                                  (fn [error]
                                    (reset! file-evidence-schema-promise nil)
                                    (js/Promise.reject error))))]
                         (reset! file-evidence-schema-promise promise)
                         promise))
                   (.finally
                    (fn []
                      (swap! timing update :neo4j-projection-ms +
                             (max 0 (- (now-ms) schema-started-at)))))
                   (.then
                    (fn [_]
                      (knowledge-index/index-session! projection-input)))
                   (.then
                    (fn [repository]
                      (if repository
                        (swap! runtime-state assoc
                               :file-evidence-indexed-at (.toISOString (js/Date.))
                               :file-evidence-repository (or (:normalized-remote repository)
                                                             (:root repository))
                               :file-evidence-status nil
                               :file-evidence-error nil)
                        (swap! runtime-state assoc
                               :file-evidence-indexed-at nil
                               :file-evidence-repository nil
                               :file-evidence-status "waiting for a Git cwd or explicit file-tool path"
                               :file-evidence-error nil))
                      nil))
                   ))))
         isolate-projection-error!
         (fn [error]
           (swap! runtime-state assoc
                  :file-evidence-status nil
                  :file-evidence-error (error-message error))
           nil)
         synchronize-file!
         (fn [replica user path selected-leaf-id selection-known? timing]
           (let [sync-input (cond-> {:path path
                                     :user-uuid (:user-uuid user)
                                     :replica replica}
                              selection-known? (assoc :current-leaf-id selected-leaf-id))
                 replica-started-at (now-ms)]
             (-> (js/Promise.resolve nil)
                 (.then (fn [_] (sync/sync-session-file! sync-input)))
                 (.finally
                  (fn []
                    (record-duration! timing :replica-sync-ms replica-started-at)))
                 (.then
                  (fn [result]
                    (-> (project-file-evidence!
                         replica path (:user-uuid user) selected-leaf-id selection-known? timing)
                        (.catch isolate-projection-error!)
                        (.then (fn [_] result))))))))
         code-memory-rebuild-promise (atom nil)
         rebuild-code-memory!
         (fn [replica user timing]
           (if-not (satisfies? knowledge-store/CodeMemoryMigrationStore replica)
             (js/Promise.resolve {:status :unsupported})
             (or @code-memory-rebuild-promise
                 (let [user-id (identity/user-urn (:user-uuid user))
                       promise
                       (-> (knowledge-migration/rebuild-if-needed!
                            {:store replica
                             :user-id user-id
                             :session-files (or (:list-code-memory-session-files options)
                                                commands/all-session-files)
                             :rebuild-session!
                             (fn [path]
                               (-> (sync/sync-session-file!
                                    {:path path
                                     :user-uuid (:user-uuid user)
                                     :replica replica})
                                   (.then
                                    (fn [_]
                                      (project-file-evidence!
                                       replica path (:user-uuid user) nil false timing)))))})
                           (.then
                            (fn [result]
                              (swap! runtime-state assoc
                                     :code-memory-version (:version result)
                                     :code-memory-rebuild-status (name (:status result))
                                     :code-memory-rebuild-error nil)
                              result))
                           (.catch
                            (fn [error]
                              (reset! code-memory-rebuild-promise nil)
                              (swap! runtime-state assoc
                                     :code-memory-rebuild-status "waiting to retry"
                                     :code-memory-rebuild-error (error-message error))
                              (js/Promise.reject error))))]
                   (reset! code-memory-rebuild-promise promise)
                   promise))))
         record-replication-error!
         (fn [ctx error]
           (swap! runtime-state assoc :connected? false :last-error (error-message error))
           (when-not @warned-unavailable?
             (notify! ctx "adam session replication unavailable; local JSONL remains authoritative" "warning")
             (reset! warned-unavailable? true))
           {:status :replication-failed})
         run-sync!
         (fn [ctx timing]
           (if-not (:enabled? resolved-config)
             (js/Promise.resolve nil)
             (if-let [path (session-file ctx)]
               (do
                 (swap! runtime-state assoc :active-session-file path)
                 (let [initialization-started-at (now-ms)]
                   (-> (js/Promise.all #js [(get-replica!)
                                          (get-user!)
                                          (resolve-git-identity! ctx)])
                     (.then
                      (fn [resolved]
                        (let [replica (aget resolved 0)
                              user (aget resolved 1)
                              git-identity (aget resolved 2)
                              initialize-identity
                              (if (and git-identity
                                       (not (contains? @initialized-identities
                                                       (:id git-identity))))
                                (-> (store/initialize!
                                     replica
                                     {:id (identity/user-urn (:user-uuid user))
                                      :identity git-identity})
                                    (.then
                                     (fn [_]
                                       (swap! initialized-identities conj (:id git-identity)))))
                                (js/Promise.resolve nil))]
                          (when git-identity
                            (swap! runtime-state assoc
                                   :git-email (:normalized-value git-identity)))
                          (-> initialize-identity
                              (.then
                               (fn [_]
                                 (rebuild-code-memory! replica user timing)))
                              (.then
                               (fn [_]
                                 (record-duration! timing :initialization-ms
                                                   initialization-started-at)
                                 (synchronize-file!
                                  replica user path (current-leaf-id ctx) true timing)))))))
                     (.then
                      (fn [result]
                        (swap! runtime-state assoc
                               :connected? true
                               :reason nil
                               :last-result result
                               :last-mirrored-at (.toISOString (js/Date.))
                               :last-error nil)
                        (when @warned-unavailable?
                          (notify! ctx "adam session replication recovered" "info")
                          (reset! warned-unavailable? false))
                        nil))
                     (.catch
                      (fn [error]
                        (when (zero? (:initialization-ms @timing))
                          (record-duration! timing :initialization-ms
                                            initialization-started-at))
                        (record-replication-error! ctx error))))))
               (do
                 (swap! runtime-state assoc
                        :connected? false
                        :reason "active session is ephemeral"
                        :active-session-file nil)
                 (js/Promise.resolve nil)))))
         drain-notifications!
         (fn [replica user recover-expired?]
           (if (satisfies? knowledge-store/MemoryReconciliationStore replica)
             (memory-service/drain-after-transcripts!
               #(notification-worker/drain-once!
                  {:inbox-options (:worker-inbox-options options)
                   :process! (fn [notification]
                               (claude-reconcile/reconcile!
                                 {:locator-options (:worker-inbox-options options)
                                  :notification notification :store replica :user-uuid (:user-uuid user)
                                  :resolve-repository! (fn [cwd _] (resolve-repository! cwd (:user-uuid user)))}))})
               #(-> (memory-service/drain!
                       {:store replica :user-uuid (:user-uuid user)
                        :inbox-options (:memory-inbox-options options)
                        :recover-expired? recover-expired?})
                    (.then (fn [result]
                             (swap! runtime-state assoc :memory-notifications result :memory-notifications-error nil)
                             result))))
             (js/Promise.resolve {:processed 0 :pending 0})))
         synchronize!
         (fn synchronize!
           ([ctx] (synchronize! ctx :manual))
           ([ctx event]
            (let [enqueued-at (now-ms)
                  timing (atom {:event event
                                :queue-wait-ms 0
                                :initialization-ms 0
                                :replica-sync-ms 0
                                :repository-discovery-ms 0
                                :evidence-extraction-ms 0
                                :neo4j-projection-ms 0
                                :total-ms 0})
                  pre-initialization-ms (atom 0)
                  next-run
                  (.then
                   @queue
                   (fn [_]
                     (record-duration! timing :queue-wait-ms enqueued-at)
                     (-> (.then (js/Promise.resolve nil) (fn [_] (run-sync! ctx timing)))
                         ;; Source writes must run even while the detached worker
                         ;; owns the notification lease. Classify their failures
                         ;; independently from later notification-drain failures.
                         (.catch (fn [error] (record-replication-error! ctx error)))
                         (.then
                           (fn [result]
                             (when-not (= :replication-failed (:status result))
                               (-> (.then (js/Promise.resolve nil)
                                     (fn [_]
                                       (when (and (:enabled? resolved-config)
                                                  (or (= event :reconcile)
                                                      (session-file ctx)
                                                      (seq (memory-inbox/pending (:memory-inbox-options options)))
                                                      (seq (memory-inbox/rejections (:memory-inbox-options options)))
                                                      (seq (transcript-inbox/pending (:worker-inbox-options options)))))
                                         (let [started-at (now-ms)]
                                           (-> (get-replica!)
                                               (.finally #(reset! pre-initialization-ms
                                                                  (max 0 (- (now-ms) started-at))))
                                               (.then
                                                 (fn [replica]
                                                   (when (satisfies? knowledge-store/MemoryReconciliationStore replica)
                                                     (notification-worker/with-lease!
                                                       (:worker-inbox-options options)
                                                       #(-> (get-user!)
                                                            (.then (fn [user]
                                                                     (drain-notifications! replica user (= event :reconcile))))))))))))))
                                   (.catch (fn [error]
                                             (swap! runtime-state assoc :memory-notifications-error
                                                    (if (= :worker-busy (:reason (ex-data error)))
                                                      "Worker busy; notification drains deferred to a later entry point"
                                                      "Reconciliation deferred; pending notifications retained"))))))))
                         (.finally
                          (fn []
                            (swap! timing update :initialization-ms + @pre-initialization-ms)
                            (record-duration! timing :total-ms enqueued-at)
                            (swap! runtime-state assoc
                                   :last-timing
                                   (assoc @timing
                                          :completed-at (.toISOString (js/Date.)))))))))]
              (reset! queue next-run)
              next-run)))
         import-file!
         (fn [path ctx]
           (if-not (:enabled? resolved-config)
             (js/Promise.reject (js/Error. (:reason resolved-config)))
             (let [result
                   (.then
                    @queue
                    (fn [_]
                      (-> (js/Promise.all #js [(get-replica!) (get-user!)])
                          (.then
                           (fn [resolved]
                             (synchronize-file!
                               (aget resolved 0) (aget resolved 1) path nil false
                               (atom {:event :import :queue-wait-ms 0
                                      :initialization-ms 0 :replica-sync-ms 0
                                      :repository-discovery-ms 0 :evidence-extraction-ms 0
                                      :neo4j-projection-ms 0 :total-ms 0})))))))]
               (reset! queue (.then result (fn [_] nil) (fn [_] nil)))
               result)))
         query-dependencies
         {:get-store! get-replica!
          :get-user! get-user!
          :resolve-repository! resolve-repository!}
         shutdown!
         (fn []
           (-> @queue
               (.then
                (fn [_]
                  (if-let [promise @replica-promise]
                    (-> promise
                        (.then (fn [replica] (store/close! replica)))
                        (.catch (fn [_] nil)))
                    nil)))
               (.then
                (fn [_]
                  (reset! replica-promise nil)
                  nil))))]
     (invoke pi
             "registerCommand"
             "adam:status"
             #js {:description "Show adam status"
                  :handler (fn [_args ctx]
                             (notify! ctx (render-status @runtime-state) "info"))})
     (commands/register!
      pi
      {:config resolved-config
       :import-file! import-file!
       :get-replica! get-replica!
       :get-user! get-user!
       :list-sessions! (:list-sessions options)})
     (invoke pi "registerCommand" "adam:reconcile"
             #js {:description "Reconcile notifications and recover parked locators with owned sources once"
                  :handler (fn [_args ctx]
                             (-> (synchronize! ctx :reconcile)
                                 (.then (fn [_]
                                          (notify! ctx "adam reconciliation attempted; inspect /adam:status for pending work" "info")))))})
     (knowledge-surfaces/register-command! pi query-dependencies)
     (when (:enabled? resolved-config)
       (knowledge-surfaces/register-tool! pi query-dependencies)
       (doseq [event ["session_start"
                      "turn_end"
                      "session_compact"
                      "session_tree"
                      "session_info_changed"
                      "model_select"
                      "thinking_level_select"]]
         (invoke pi
                 "on"
                 event
                 (fn [_event ctx]
                   ;; Pi interprets turn_end results as boundary drafts. Internal
                   ;; ClojureScript maps have an entries() method, not draft data.
                   (-> (synchronize! ctx (keyword (string/replace event "_" "-")))
                       (.then (fn [_] js/undefined))))))
       (invoke pi "on" "session_shutdown" (fn [_event _ctx] (shutdown!))))
     {:status (fn [] @runtime-state)
      :synchronize! synchronize!
      :import-file! import-file!
      :shutdown! shutdown!})))
