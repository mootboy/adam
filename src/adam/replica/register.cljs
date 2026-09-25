(ns adam.replica.register
  (:require [adam.knowledge.index :as knowledge-index]
            [adam.knowledge.repository :as knowledge-repository]
            [adam.knowledge.store :as knowledge-store]
            [adam.replica.commands :as commands]
            [adam.replica.config :as config]
            [adam.replica.identity :as identity]
            [adam.replica.neo4j :as neo4j]
            [adam.replica.state :as global-state]
            [adam.replica.store :as store]
            [adam.replica.sync :as sync]
            [clojure.string :as string]
            ["node:os" :refer [homedir]]
            ["node:path" :as node-path :refer [join]]))

(def ^:private environment-keys
  ["ADAM_NEO4J_URI"
   "ADAM_NEO4J_USERNAME"
   "ADAM_NEO4J_PASSWORD"
   "ADAM_NEO4J_DATABASE"])

(defn- agent-dir []
  (let [configured (aget js/process.env "PI_CODING_AGENT_DIR")]
    (cond
      (and configured (string/starts-with? configured "~/"))
      (.resolve node-path (homedir) (subs configured 2))

      (and configured (not (string/blank? configured))) configured
      :else (join (homedir) ".pi" "agent"))))

(defn- process-environment []
  (into {}
        (map (fn [key] [key (aget js/process.env key)]))
        environment-keys))

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

(defn- render-status [runtime-state]
  (let [{:keys [configured? connected? reason user-uuid git-email active-session-file
                last-result last-mirrored-at last-error file-evidence-indexed-at
                file-evidence-repository file-evidence-status file-evidence-error]} runtime-state]
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
              (when file-evidence-indexed-at
                (str "File evidence indexed: " file-evidence-indexed-at))
              (when file-evidence-repository
                (str "File evidence repository: " file-evidence-repository))
              (when file-evidence-status (str "File evidence: " file-evidence-status))
              (when file-evidence-error (str "File evidence error: " file-evidence-error))]))))

(defn register!
  ([pi]
   (register! pi {}))
  ([pi options]
   (let [resolved-config (or (:config options)
                             (config/resolve-config (process-environment)))
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
                       #(global-state/load-or-create! (agent-dir)))
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
                                   (-> (store/initialize!
                                        replica
                                        {:id (identity/user-urn (:user-uuid user))})
                                       (.then (fn [_] replica))))))))
                         (.catch
                          (fn [error]
                            (reset! replica-promise nil)
                            (js/Promise.reject error))))]
                 (reset! replica-promise promise)
                 promise)))
         project-file-evidence!
         (fn [replica path user-uuid selected-leaf-id selection-known?]
           (if-not (satisfies? knowledge-store/FileEvidenceStore replica)
             (js/Promise.resolve nil)
             (let [projection-input
                   (cond-> {:store replica
                            :path path
                            :user-uuid user-uuid
                            :resolve-repository #(resolve-repository! % user-uuid)}
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
                   (.then (fn [_] (knowledge-index/index-session! projection-input)))
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
                   (.catch
                    (fn [error]
                      (swap! runtime-state assoc
                             :file-evidence-status nil
                             :file-evidence-error (error-message error))
                      nil))))))
         synchronize-file!
         (fn [replica user path selected-leaf-id selection-known?]
           (let [sync-input (cond-> {:path path
                                     :user-uuid (:user-uuid user)
                                     :replica replica}
                              selection-known? (assoc :current-leaf-id selected-leaf-id))]
             (-> (sync/sync-session-file! sync-input)
                 (.then
                  (fn [result]
                    (-> (project-file-evidence!
                         replica path (:user-uuid user) selected-leaf-id selection-known?)
                        (.then (fn [_] result))))))))
         run-sync!
         (fn [ctx]
           (if-not (:enabled? resolved-config)
             (js/Promise.resolve nil)
             (if-let [path (session-file ctx)]
               (do
                 (swap! runtime-state assoc :active-session-file path)
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
                                 (synchronize-file!
                                  replica user path (current-leaf-id ctx) true)))))))
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
                        (swap! runtime-state assoc
                               :connected? false
                               :last-error (error-message error))
                        (when-not @warned-unavailable?
                          (notify! ctx
                                   "adam session replication unavailable; local JSONL remains authoritative"
                                   "warning")
                          (reset! warned-unavailable? true))
                        nil))))
               (do
                 (swap! runtime-state assoc
                        :connected? false
                        :reason "active session is ephemeral"
                        :active-session-file nil)
                 (js/Promise.resolve nil)))))
         synchronize!
         (fn [ctx]
           (let [next-run (.then @queue (fn [_] (run-sync! ctx)))]
             (reset! queue next-run)
             next-run))
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
                              (aget resolved 0)
                              (aget resolved 1)
                              path
                              nil
                              false))))))]
               (reset! queue (.then result (fn [_] nil) (fn [_] nil)))
               result)))
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
     (when (:enabled? resolved-config)
       (doseq [event ["session_start"
                      "turn_end"
                      "session_compact"
                      "session_tree"
                      "session_info_changed"
                      "model_select"
                      "thinking_level_select"]]
         (invoke pi "on" event (fn [_event ctx] (synchronize! ctx))))
       (invoke pi "on" "session_shutdown" (fn [_event _ctx] (shutdown!))))
     {:status (fn [] @runtime-state)
      :synchronize! synchronize!
      :import-file! import-file!
      :shutdown! shutdown!})))
