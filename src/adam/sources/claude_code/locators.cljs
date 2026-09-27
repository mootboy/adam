(ns adam.sources.claude-code.locators
  (:refer-clojure :exclude [load])
  (:require ["node:crypto" :refer [createHash randomUUID]]
            ["node:fs" :refer [chmodSync closeSync fsyncSync mkdirSync openSync
                               readFileSync renameSync rmSync writeFileSync]]
            ["node:os" :refer [homedir]]
            ["node:path" :refer [join]]))

(def ^:private locator-version 1)

(defn- config-home [options]
  (or (:config-home options)
      (aget js/process.env "XDG_CONFIG_HOME")
      (join (homedir) ".config")))

(defn- locator-dir [options]
  (join (config-home options) "adam" "claude-sessions"))

(defn- sha256 [value]
  (-> (createHash "sha256") (.update value) (.digest "hex")))

(defn- locator-path [options session-id]
  (join (locator-dir options) (str (sha256 session-id) ".json")))

(defn- read-if-present [path]
  (try
    (js->clj (js/JSON.parse (readFileSync path "utf8")) :keywordize-keys true)
    (catch :default error
      (if (= "ENOENT" (.-code error)) nil (throw error)))))

(defn load [options session-id]
  (read-if-present (locator-path options session-id)))

(defn- write-atomic! [directory path value]
  (let [temporary-path (str path "." js/process.pid "." (randomUUID) ".tmp")
        payload (str (js/JSON.stringify (clj->js value)) "\n")]
    (mkdirSync directory #js {:recursive true :mode 448})
    (let [fd (openSync temporary-path "wx" 384)]
      (try
        (writeFileSync fd payload "utf8")
        (fsyncSync fd)
        (finally (closeSync fd))))
    (renameSync temporary-path path)
    (chmodSync path 384)
    (let [fd (openSync directory "r")]
      (try (fsyncSync fd) (finally (closeSync fd))))
    (rmSync temporary-path #js {:force true})))

(defn record!
  [options {:keys [event session-id transcript-path parent-transcript-path
                   agent-id cwd]}]
  (let [directory (locator-dir options)
        path (locator-path options session-id)
        existing (or (read-if-present path)
                     {:version locator-version
                      :session-id session-id
                      :subagents []})
        parent-path (if (= event "SubagentStop")
                      parent-transcript-path
                      transcript-path)
        updated (cond-> (assoc existing
                               :version locator-version
                               :session-id session-id
                               :transcript-path (or parent-path (:transcript-path existing))
                               :cwd cwd)
                  (= event "SubagentStop")
                  (assoc :subagents
                         (->> (conj (remove #(= agent-id (:agent-id %))
                                            (:subagents existing))
                                    {:agent-id agent-id
                                     :transcript-path transcript-path})
                              (sort-by :agent-id)
                              vec)))]
    (write-atomic! directory path updated)
    updated))
