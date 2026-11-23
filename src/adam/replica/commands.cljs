(ns adam.replica.commands
  (:require [adam.replica.config :as config]
            [adam.replica.identity :as identity]
            [adam.replica.jsonl :as jsonl]
            [adam.replica.restore :as restore]
            [adam.replica.store :as store]
            [clojure.string :as string]
            ["node:buffer" :refer [Buffer]]
            ["node:fs" :refer [readdirSync]]
            ["node:module" :refer [createRequire findPackageJSON]]
            ["node:os" :refer [homedir]]
            ["node:path" :as node-path :refer [join]]
            ["neo4j-driver" :as neo4j-driver]))

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

(defn all-session-files []
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
                                                   js/undefined)
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

(defn- password-prompt! [ctx module-url]
  ;; Resolve Pi's existing TUI through its peer package, only for interactive
  ;; setup. Neither MCP nor startup depends on a loaded TUI or a new dependency.
  (let [host-require (createRequire (findPackageJSON "@earendil-works/pi-coding-agent"
                                                    (or module-url (join (.cwd js/process) "extension.js"))))
        module (host-require "@earendil-works/pi-tui")]
    ;; Node >=22.19 supports require(ESM); no dynamic-import transpilation or
    ;; extra compiled TUI copy is needed.
    (-> (js/Promise.resolve module)
        (.then
         (fn [module]
           (invoke (aget ctx "ui") "custom"
                   (fn [tui _theme keybindings done]
                     ;; Use the host Input for text editing, paste and terminal
                     ;; protocols, but NEVER call its plaintext renderer. Our
                     ;; component shows no text cursor, only an entered indicator.
                     (let [input (js/Reflect.construct (aget module "Input") #js [])
                           finish! (fn [result] (invoke input "setValue" "") (done result))]
                       (aset input "onSubmit" finish!)
                       (aset input "onEscape" #(finish! nil))
                       #js {:render (fn [width]
                                      (clj->js (map #(subs % 0 (min (max 0 width) (count %)))
                                                    ["Adam password (hidden)"
                                                     (str "Password: " (if (seq (invoke input "getValue")) "[entered]" "[empty]"))
                                                     "Enter submits; Esc cancels; Backspace edits; Ctrl+U clears"])))
                            :invalidate (fn [])
                            :dispose (fn [] (invoke input "setValue" ""))
                            :handleInput
                            (fn [data]
                              (if (or (invoke keybindings "matches" data "tui.select.cancel") (= data "\u0003"))
                                (finish! nil)
                                (invoke input "handleInput" data))
                              (when (> (.byteLength Buffer (invoke input "getValue") "utf8") 8192)
                                (notify! ctx "Password exceeds 8192 UTF-8 bytes; setup cancelled." "error")
                                (finish! nil))
                              (invoke tui "requestRender"))}))))))))

(defn- test-connection! [settings signal create-driver]
  ;; A dedicated bounded read-only probe, never the replica initializer.
  (let [auth-api (.-auth neo4j-driver)
        factory (or create-driver #(.driver neo4j-driver %1 %2 %3))
        driver (factory (:uri settings)
                        (.basic auth-api (:username settings) (:password settings))
                        #js {:connectionTimeout 5000 :connectionAcquisitionTimeout 5000
                             :maxTransactionRetryTime 0 :logging #js {:level "error" :logger (fn [])}})
        session (atom nil)
        closing (atom nil)
        close! (fn []
                 (or @closing
                     ;; Never wait for RESET/session closure before shutting
                     ;; down the driver. Driver.close destroys active sockets,
                     ;; including a connection whose peer never answers RESET.
                     (let [work (js/Promise.all
                                 #js [(-> (js/Promise.resolve nil)
                                          (.then (fn [_] (when-let [active @session] (.close active)))))
                                      (-> (js/Promise.resolve nil)
                                          (.then (fn [_] (.close driver))))])]
                       (reset! closing work) work)))
        abort! (fn [] (-> (close!) (.catch (fn [_] nil))))]
    (.addEventListener signal "abort" abort! #js {:once true})
    (-> (js/Promise.resolve nil)
        (.then (fn [_]
                 (when (.-aborted signal) (throw (js/Error. "cancelled")))
                 (let [active (.session driver #js {:database (:database settings) :defaultAccessMode "READ"})]
                   (reset! session active)
                   (.run active "RETURN 1 AS ok" #js {} #js {:timeout 5000}))))
        (.then (fn [_] true))
        (.finally (fn [] (.removeEventListener signal "abort" abort!) (close!))))))

(defn- probe-prompt! [ctx probe! settings timeout-ms]
  (invoke (aget ctx "ui") "custom"
          (fn [tui _theme keybindings done]
            (let [controller (js/AbortController.)
                  signal (.-signal controller)
                  cancel! (atom nil)
                  cancelled (js/Promise. (fn [resolve _]
                                           (let [listener #(resolve :cancelled)]
                                             (reset! cancel! listener)
                                             (.addEventListener signal "abort" listener #js {:once true}))))
                  timer (js/setTimeout #(.abort controller) timeout-ms)
                  probe (-> (js/Promise.resolve nil)
                            (.then (fn [_] (when-not (.-aborted signal) (probe! settings signal))))
                            (.then (fn [_] (if (.-aborted signal) :cancelled :success)))
                            (.catch (fn [_] (if (.-aborted signal) :cancelled :failed))))
                  work (js/Promise.race #js [probe cancelled])]
              ;; The losing probe remains rejection-handled, but never keeps the
              ;; terminal modal open while query or cleanup promises are pending.
              (-> work
                  (.then done)
                  (.finally (fn [] (js/clearTimeout timer)
                                   (.removeEventListener signal "abort" @cancel!))))
              #js {:render (fn [width]
                             (let [label "Testing Adam connection (read-only); Esc cancels"]
                               #js [(subs label 0 (min (max 0 width) (count label)))]))
                   :invalidate (fn [])
                   :dispose (fn [] (js/clearTimeout timer) (.abort controller))
                   :handleInput (fn [data]
                                  (when (or (invoke keybindings "matches" data "tui.select.cancel")
                                            (= data "\u0003"))
                                    (.abort controller)
                                    (invoke tui "requestRender")))}))))

(defn- configure! [ctx {:keys [configure-read! configure-save! configure-test! configure-driver! configure-module-url configure-probe-timeout-ms]}]
  (let [ui (aget ctx "ui")
        read! (or configure-read! config/read-connection!)
        save! (or configure-save! config/save-connection!)
        probe! (or configure-test! #(test-connection! %1 %2 configure-driver!))]
    (-> (js/Promise.resolve nil)
        (.then (fn [_]
                 (when (config/environment-configured? (config/process-environment))
                   (notify! ctx "ADAM_NEO4J_* overrides the saved connection. Unset all four variables to use the file after restart." "warning"))
                 (read!)))
        (.then
         (fn [saved]
           (reduce
            (fn [promise [key label fallback]]
              (.then promise
                     (fn [settings]
                       (when settings
                         (-> (invoke ui "input" label (get saved key fallback))
                             (.then (fn [answer]
                                      (when (some? answer)
                                        (assoc settings key (if (string/blank? answer)
                                                              (get saved key fallback) answer))))))))))
            (js/Promise.resolve {:version 1})
            [[:uri "Neo4j URI (Enter keeps default)" "bolt://127.0.0.1:7687"]
             [:username "Neo4j username (Enter keeps default)" "neo4j"]
             [:database "Neo4j database (Enter keeps default)" "neo4j"]])))
        (.then (fn [settings]
                 (when settings
                   (-> (password-prompt! ctx configure-module-url)
                       (.then (fn [password]
                                (when (some? password)
                                  (config/normalize-connection (assoc settings :password password)))))))))
        (.then
         (fn [settings]
           (when settings
             (letfn [(choose! []
                       (-> (invoke ui "select" "Adam connection — password stored as private plaintext (0600)"
                                   #js ["Test connection" "Save" "Cancel"])
                           (.then
                            (fn [action]
                              (case action
                                "Save" (do (save! settings)
                                           (notify! ctx "Adam connection saved privately. Restart Pi, Claude MCP and detached workers to apply; the current connection is unchanged." "info"))
                                "Test connection"
                                (-> (probe-prompt! ctx probe! settings (or configure-probe-timeout-ms 15000))
                                    (.then (fn [result]
                                             (case result
                                               :success (do (notify! ctx "Adam connection test succeeded; nothing saved yet." "info")
                                                            (choose!))
                                               :failed (notify! ctx "Adam connection test failed; no settings saved." "error")
                                               (notify! ctx "Adam connection test cancelled; no settings saved." "info")))))
                                nil)))))]
               (choose!)))))
        (.catch (fn [error]
                  (notify! ctx
                           (if (true? (::config/published? (ex-data error)))
                             "Adam connection file was replaced, but crash durability could not be confirmed. Check connection.yaml before retrying. Restart hosts to apply any saved settings; the current connection is unchanged."
                             "Adam configuration failed; no settings saved. Check private file permissions and connection settings. Connection tests never run migrations.")
                           "error"))))))

(defn- register-configure! [pi dependencies]
  (invoke pi "registerCommand" "adam:configure"
          #js {:description "Save a private Adam connection for subsequent host launches"
               :handler (fn [arguments ctx]
                          (cond
                            (not (string/blank? arguments))
                            (notify! ctx "Usage: /adam:configure (no arguments; never put credentials in commands)" "error")
                            (not (and (true? (aget ctx "hasUI")) (= "tui" (aget ctx "mode"))))
                            (notify! ctx "Adam configuration requires Pi terminal UI; use a private connection.yaml in other hosts." "error")
                            :else (configure! ctx dependencies)))}))

(defn register! [pi dependencies]
  (register-configure! pi dependencies)
  (register-import! pi dependencies)
  (register-resume! pi dependencies))
