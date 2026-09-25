(ns adam.replica.commands
  (:require [adam.replica.identity :as identity]
            [adam.replica.jsonl :as jsonl]
            [adam.replica.restore :as restore]
            [adam.replica.store :as store]
            [clojure.string :as string]
            ["node:fs" :refer [readdirSync]]
            ["node:os" :refer [homedir]]
            ["node:path" :as node-path :refer [join]]))

(defn- invoke [object method & arguments]
  (.apply (aget object method) object (to-array arguments)))

(defn- notify! [ctx message level]
  (invoke (aget ctx "ui") "notify" message level))

(defn- value [item key property]
  (if (map? item) (get item key) (aget item property)))

(defn- as-items [items]
  (if (js/Array.isArray items) (array-seq items) items))

(defn- configured-agent-dir []
  (let [configured (aget js/process.env "PI_CODING_AGENT_DIR")]
    (cond
      (and configured (string/starts-with? configured "~/"))
      (.resolve node-path (homedir) (subs configured 2))

      (and configured (not (string/blank? configured))) configured
      :else (join (homedir) ".pi" "agent"))))

(defn- jsonl-files [directory]
  (try
    (->> (array-seq (readdirSync directory #js {:withFileTypes true}))
         (filter (fn [^js entry]
                   (and (.isFile entry) (.endsWith (.-name entry) ".jsonl"))))
         (map (fn [^js entry] (join directory (.-name entry)))))
    (catch :default _
      [])))

(defn- all-session-files []
  (let [sessions-directory (join (configured-agent-dir) "sessions")]
    (try
      (->> (array-seq (readdirSync sessions-directory #js {:withFileTypes true}))
           (filter (fn [^js entry]
                     (or (.isDirectory entry) (.isSymbolicLink entry))))
           (mapcat (fn [^js entry]
                     (jsonl-files (join sessions-directory (.-name entry))))))
      (catch :default _
        []))))

(defn- discovered-session [path]
  (try
    (let [header (jsonl/read-session-header path)]
      {:id (:pi-session-id header)
       :path path
       :cwd (:cwd header)})
    (catch :default _
      {:path path})))

(defn- default-list-sessions! [all? cwd session-dir]
  (let [resolved-cwd (.resolve node-path cwd)
        paths (if all? (all-session-files) (jsonl-files session-dir))
        sessions (->> paths
                      sort
                      (map discovered-session)
                      (filter #(or all?
                                   (nil? (:cwd %))
                                   (= resolved-cwd (.resolve node-path (:cwd %)))))
                      vec)]
    (js/Promise.resolve sessions)))

(defn- malformed-session-error? [error]
  (= "SessionJsonlError" (.-name error)))

(defn- register-import! [pi {:keys [config import-file! list-sessions!]}]
  (invoke
   pi
   "registerCommand"
   "adam:import"
   #js {:description "Import local Pi sessions into the adam Neo4j replica"
        :handler
        (fn [arguments ctx]
          (if-not (:enabled? config)
            (do
              (notify! ctx (str "adam import unavailable: " (:reason config)) "error")
              (js/Promise.resolve nil))
            (let [normalized (string/trim arguments)]
              (cond
                (not (contains? #{"" "--all"} normalized))
                (do
                  (notify! ctx "Usage: /adam:import [--all]" "error")
                  (js/Promise.resolve nil))

                (not= true (aget ctx "hasUI"))
                (do
                  (notify! ctx "adam import requires interactive confirmation" "error")
                  (js/Promise.resolve nil))

                :else
                (-> (invoke ctx "waitForIdle")
                    (.then
                     (fn [_]
                       ((or list-sessions! default-list-sessions!)
                        (= "--all" normalized)
                        (aget ctx "cwd")
                        (invoke (aget ctx "sessionManager") "getSessionDir"))))
                    (.then
                     (fn [discovered]
                       (let [sessions
                             (vals
                              (reduce
                               (fn [by-path item]
                                 (let [path (value item :path "path")]
                                   (if path (assoc by-path path item) by-path)))
                               {}
                               (as-items discovered)))]
                         (-> (invoke (aget ctx "ui")
                                     "confirm"
                                     "Import Pi sessions into adam?"
                                     (str (count sessions)
                                          " session(s) will be uploaded with complete message, tool, and image content."))
                             (.then
                              (fn [confirmed?]
                                (when confirmed?
                                  (let [counts (atom {:mirrored 0
                                                      :unchanged 0
                                                      :conflict 0
                                                      :malformed 0
                                                      :failed 0})
                                        import-chain
                                        (reduce
                                         (fn [promise [index item]]
                                           (.then
                                            promise
                                            (fn [_]
                                              (invoke (aget ctx "ui")
                                                      "setStatus"
                                                      "adam-import"
                                                      (str "Importing " (inc index) "/" (count sessions)))
                                              (-> (import-file! (value item :path "path") ctx)
                                                  (.then
                                                   (fn [result]
                                                     (swap! counts update (:status result) inc)))
                                                  (.catch
                                                   (fn [error]
                                                     (swap! counts update
                                                            (if (malformed-session-error? error)
                                                              :malformed
                                                              :failed)
                                                            inc)))))))
                                         (js/Promise.resolve nil)
                                         (map-indexed vector sessions))]
                                    (-> import-chain
                                        (.then
                                         (fn [_]
                                           (invoke (aget ctx "ui")
                                                   "setStatus"
                                                   "adam-import"
                                                   nil)
                                           (let [{:keys [mirrored unchanged conflict malformed failed]} @counts
                                                 warning? (pos? (+ conflict malformed failed))]
                                             (notify!
                                              ctx
                                              (str "adam import complete: " mirrored " mirrored, "
                                                   unchanged " unchanged, " conflict " conflicted, "
                                                   malformed " malformed, " failed " failed")
                                              (if warning? "warning" "info"))))))))))))))
                    (.catch
                     (fn [error]
                       (notify! ctx (str "adam import failed: " (.-message error)) "error"))))))))}))

(defn- local-session [item]
  {:id (value item :id "id")
   :path (value item :path "path")
   :name (value item :name "name")})

(defn- register-resume!
  [pi {:keys [config get-replica! get-user! list-sessions!]}]
  (invoke
   pi
   "registerCommand"
   "adam:resume"
   #js {:description "Resume an exact-cwd Pi session from local storage or adam"
        :handler
        (fn [_arguments ctx]
          (-> (invoke ctx "waitForIdle")
              (.then
               (fn [_]
                 ((or list-sessions! default-list-sessions!)
                  false
                  (aget ctx "cwd")
                  (invoke (aget ctx "sessionManager") "getSessionDir"))))
              (.then
               (fn [discovered]
                 (let [locals (->> (as-items discovered)
                                   (map local-session)
                                   (filter :id)
                                   (map (juxt :id identity))
                                   (into {}))
                       remote-promise
                       (if (:enabled? config)
                         (-> (js/Promise.all #js [(get-replica!) (get-user!)])
                             (.then
                              (fn [resolved]
                                (store/list-sessions!
                                 (aget resolved 0)
                                 {:user-id (identity/user-urn
                                            (:user-uuid (aget resolved 1)))
                                  :cwd (aget ctx "cwd")})))
                             (.catch
                              (fn [error]
                                (notify! ctx
                                         (str "adam remote sessions unavailable: " (.-message error))
                                         "warning")
                                [])))
                         (js/Promise.resolve []))]
                   (-> remote-promise
                       (.then
                        (fn [remote-sessions]
                          (let [remote-by-id (into {} (map (juxt :pi-session-id identity)
                                                          remote-sessions))
                                ids (sort (into (set (keys locals)) (keys remote-by-id)))
                                choices
                                (mapv
                                 (fn [id]
                                   (let [local (get locals id)
                                         remote (get remote-by-id id)
                                         status (cond
                                                  (:conflicted? remote) "conflict"
                                                  (and local remote) "local+neo"
                                                  local "local"
                                                  :else "neo")
                                         name (or (:name local) (:name remote) id)]
                                     {:id id
                                      :local local
                                      :remote remote
                                      :label (str "[" status "] " name " — " id)}))
                                 ids)]
                            (if (empty? choices)
                              (notify! ctx "No adam sessions found for the current working directory" "info")
                              (-> (invoke (aget ctx "ui")
                                          "select"
                                          "Resume adam session"
                                          (clj->js (mapv :label choices)))
                                  (.then
                                   (fn [selected-label]
                                     (when-let [selected
                                                (first (filter #(= selected-label (:label %)) choices))]
                                       (let [local (:local selected)
                                             remote (:remote selected)]
                                         (cond
                                           local
                                           (do
                                             (when (:conflicted? remote)
                                               (notify! ctx
                                                        "adam conflict detected; resuming authoritative local JSONL"
                                                        "warning"))
                                             (invoke ctx "switchSession" (:path local)))

                                           (or (nil? remote)
                                               (:conflicted? remote)
                                               (not (:complete? remote)))
                                           (notify! ctx
                                                    "Remote session is incomplete or conflicted and cannot be restored"
                                                    "error")

                                           :else
                                           (-> (get-replica!)
                                               (.then
                                                (fn [replica]
                                                  (let [target-path
                                                        (join
                                                         (invoke (aget ctx "sessionManager")
                                                                 "getSessionDir")
                                                         (restore/restored-session-filename remote))]
                                                    (restore/materialize-session!
                                                     replica
                                                     (:id remote)
                                                     target-path))))
                                               (.then
                                                (fn [materialized]
                                                  (invoke
                                                   ctx
                                                   "switchSession"
                                                   (:path materialized)
                                                   #js {:withSession
                                                        (fn [replacement]
                                                          (if-let [leaf-id
                                                                   (:current-leaf-id materialized)]
                                                            (-> (invoke replacement
                                                                        "navigateTree"
                                                                        leaf-id
                                                                        #js {:summarize false})
                                                                (.catch
                                                                 (fn [_]
                                                                   (notify!
                                                                    replacement
                                                                    "Restored session, but its recorded leaf could not be selected"
                                                                    "warning"))))
                                                            (js/Promise.resolve nil)))})))
                                               (.catch
                                                (fn [error]
                                                  (notify! ctx
                                                           (str "adam restore failed: " (.-message error))
                                                           "error"))))))))))))))))))
              (.catch
               (fn [error]
                 (notify! ctx (str "adam resume failed: " (.-message error)) "error")))))}))

(defn register! [pi dependencies]
  (register-import! pi dependencies)
  (register-resume! pi dependencies))
