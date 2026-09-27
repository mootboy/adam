(ns adam.sources.claude-code.inbox
  (:require [clojure.string :as string]
            ["node:fs" :refer [chmodSync closeSync fsyncSync mkdirSync openSync
                               readFileSync readdirSync renameSync rmSync writeFileSync]]
            ["node:os" :refer [homedir]]
            ["node:path" :refer [isAbsolute join]]
            ["node:crypto" :refer [randomUUID]]))

(def ^:private notification-version 1)
(def ^:private supported-events #{"SessionStart" "Stop" "SessionEnd" "SubagentStop"})

(defn- bounded-string? [value max-length]
  (and (string? value)
       (not (string/blank? value))
       (<= (count value) max-length)))

(defn- validate-input! [{:keys [hook-event-name session-id transcript-path
                                parent-transcript-path cwd agent-id]}]
  (when-not (contains? supported-events hook-event-name)
    (throw (js/Error. "unsupported Claude hook event")))
  (when-not (bounded-string? session-id 512)
    (throw (js/Error. "Claude session_id is required")))
  (when-not (and (bounded-string? transcript-path 8192) (isAbsolute transcript-path))
    (throw (js/Error. "Claude transcript_path must be absolute")))
  (when (and (some? parent-transcript-path)
             (not (and (bounded-string? parent-transcript-path 8192)
                       (isAbsolute parent-transcript-path))))
    (throw (js/Error. "Claude parent transcript_path must be absolute")))
  (when-not (and (bounded-string? cwd 8192) (isAbsolute cwd))
    (throw (js/Error. "Claude cwd must be absolute")))
  (when (and (= "SubagentStop" hook-event-name)
             (or (not (bounded-string? agent-id 512))
                 (not (and (bounded-string? parent-transcript-path 8192)
                           (isAbsolute parent-transcript-path)))))
    (throw (js/Error. "Claude SubagentStop locators are incomplete")))
  (when (and (some? agent-id) (not (bounded-string? agent-id 512)))
    (throw (js/Error. "Claude agent_id is invalid"))))

(defn- config-home [options]
  (or (:config-home options)
      (aget js/process.env "XDG_CONFIG_HOME")
      (join (homedir) ".config")))

(defn- inbox-dir [options]
  (join (config-home options) "adam" "inbox"))

(defn worker-lock-path [options]
  (join (config-home options) "adam" "worker.lock"))

(defn- sync-directory! [path]
  (let [fd (openSync path "r")]
    (try
      (fsyncSync fd)
      (finally (closeSync fd)))))

(defn enqueue!
  [options input]
  (validate-input! input)
  (let [directory (inbox-dir options)
        id ((or (:uuid-fn options) randomUUID))
        now-ms ((or (:now-ms options) #(.now js/Date)))
        notification (cond-> {:version notification-version
                              :id id
                              :event (:hook-event-name input)
                              :session-id (:session-id input)
                              :transcript-path (:transcript-path input)
                              :cwd (:cwd input)
                              :queued-at (.toISOString (js/Date. now-ms))}
                       (:parent-transcript-path input)
                       (assoc :parent-transcript-path (:parent-transcript-path input))
                       (:agent-id input) (assoc :agent-id (:agent-id input)))
        filename (str now-ms "-" id ".json")
        path (join directory filename)
        temporary-path (str path "." js/process.pid ".tmp")
        payload (str (js/JSON.stringify (clj->js notification)) "\n")]
    (mkdirSync directory #js {:recursive true :mode 448})
    (let [fd (openSync temporary-path "wx" 384)]
      (try
        (writeFileSync fd payload "utf8")
        (fsyncSync fd)
        (finally (closeSync fd))))
    (renameSync temporary-path path)
    (chmodSync path 384)
    (sync-directory! directory)
    (assoc notification :path path)))

(defn pending [options]
  (let [directory (inbox-dir options)]
    (try
      (->> (readdirSync directory #js {:withFileTypes true})
           array-seq
           (filter #(and (.isFile %) (.endsWith (.-name %) ".json")))
           (sort-by #(.-name %))
           (mapv (fn [entry]
                   (let [path (join directory (.-name entry))
                         value (js->clj (js/JSON.parse (readFileSync path "utf8"))
                                       :keywordize-keys true)]
                     (assoc value :path path)))))
      (catch :default error
        (if (= "ENOENT" (.-code error)) [] (throw error))))))

(defn acknowledge! [options notification]
  (rmSync (:path notification))
  (sync-directory! (inbox-dir options))
  nil)
