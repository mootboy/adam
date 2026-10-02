(ns adam.memory.inbox
  (:refer-clojure :exclude [resolve])
  (:require [adam.memory.protocol :as protocol]
            [clojure.string :as string]
            ["node:buffer" :refer [Buffer]]
            ["node:crypto" :refer [randomUUID]]
            ["node:fs" :refer [closeSync constants fstatSync fsyncSync lstatSync
                               mkdirSync openSync readSync readdirSync renameSync
                               rmSync writeFileSync]]
            ["node:os" :refer [homedir]]
            ["node:path" :refer [isAbsolute join resolve dirname basename]]))

(def max-notification-bytes 4096)
(def ^:private input-keys #{:source-kind :source-session-id :producer-id})
(def ^:private wire-keys #{:version :id :sourceKind :sourceSessionId :producerId
                           :sidecarLocator :queuedAt})

(defn- fail! [reason]
  (throw (ex-info "Unsafe or invalid memory notification" {:reason reason})))

(defn- root [options key environment fallback]
  (let [path (or (get options key) (aget js/process.env environment) fallback)]
    (when-not (and (string? path) (isAbsolute path)) (fail! :relative-xdg-root))
    (resolve path)))

(defn- state-root [options]
  (root options :state-home "XDG_STATE_HOME" (join (homedir) ".local" "state")))

(defn- data-root [options]
  (root options :data-home "XDG_DATA_HOME" (join (homedir) ".local" "share")))

(defn- owner-only! [stat]
  (when (or (not (zero? (bit-and 63 (.-mode stat))))
            (and (.-getuid js/process) (not= (.getuid js/process) (.-uid stat))))
    (fail! :unsafe-permissions)))

(defn- directory! [path create?]
  (when create?
    (try (mkdirSync path #js {:mode 448})
         (catch :default error
           (when-not (= "EEXIST" (.-code error)) (throw error)))))
  (let [stat (lstatSync path)]
    (when (or (.isSymbolicLink stat) (not (.isDirectory stat)))
      (fail! :unsafe-path))
    (owner-only! stat))
  path)

(defn- spool-dir [options create?]
  (when-not (contains? #{"linux" "darwin"} (.-platform js/process))
    (fail! :unsupported-owner-only-platform))
  (let [root (state-root options)]
    ;; The selected XDG root itself may be shared; every Adam component below it
    ;; must be a private real directory. Never recursively traverse a symlink.
    (when create? (mkdirSync root #js {:recursive true :mode 448}))
    (directory! (join root "adam") create?)
    (directory! (join root "adam" "memory-inbox") create?)))

(defn canonical-locator [{:keys [source-kind source-session-id producer-id] :as input}]
  (when-not (protocol/valid-locator? input) (fail! :invalid-locator))
  (str source-kind "/" (protocol/sha256 source-session-id) "/" producer-id ".jsonl"))

(defn sidecar-path [options notification]
  (let [locator (canonical-locator notification)]
    (when-not (= locator (:sidecar-locator notification)) (fail! :locator-mismatch))
    (join (data-root options) "adam" "memories" "v1" locator)))

(defn- sync-directory! [path]
  (let [fd (openSync path "r")]
    (try (fsyncSync fd) (finally (closeSync fd)))))

(defn enqueue! [options input]
  (when-not (= input-keys (set (keys input))) (fail! :unexpected-fields))
  (let [locator (canonical-locator input)
        id (randomUUID)
        queued-at (.toISOString (js/Date.))
        wire {:version 1 :id id
              :sourceKind (:source-kind input) :sourceSessionId (:source-session-id input)
              :producerId (:producer-id input) :sidecarLocator locator :queuedAt queued-at}
        payload (str (js/JSON.stringify (clj->js wire)) "\n")
        _ (when (> (.byteLength Buffer payload "utf8") max-notification-bytes)
            (fail! :oversized-notification))
        directory (spool-dir options true)
        path (join directory (str id ".json"))
        temporary (str path "." js/process.pid ".tmp")]
    (try
      (let [fd (openSync temporary "wx" 384)]
        (try (writeFileSync fd payload "utf8") (fsyncSync fd)
             (finally (closeSync fd))))
      (renameSync temporary path)
      (sync-directory! directory)
      (assoc input :version 1 :id id :queued-at queued-at :sidecar-locator locator :path path)
      (finally (rmSync temporary #js {:force true})))))

(defn- read-bounded! [fd]
  (let [buffer (.alloc Buffer (inc max-notification-bytes))]
    (loop [offset 0]
      (let [read (readSync fd buffer offset (- (.-length buffer) offset) nil)
            next-offset (+ offset read)]
        (cond
          (> next-offset max-notification-bytes) (fail! :oversized-notification)
          (zero? read) (.subarray buffer 0 offset)
          :else (recur next-offset))))))

(defn- decode! [path]
  (let [before (lstatSync path)]
    (when (or (.isSymbolicLink before) (not (.isFile before))) (fail! :unsafe-path))
    (owner-only! before)
    (when (> (.-size before) max-notification-bytes) (fail! :oversized-notification))
    (let [fd (openSync path (bit-or (.-O_RDONLY constants) (.-O_NOFOLLOW constants)))]
      (try
        (let [stat (fstatSync fd)
              _ (when (or (not (.isFile stat)) (> (.-size stat) max-notification-bytes)
                          (not= (.-ino before) (.-ino stat))
                          (not= (.-dev before) (.-dev stat))) (fail! :changed-notification))
              _ (owner-only! stat)
              payload (read-bounded! fd)
              _ (when (> (.-length payload) max-notification-bytes)
                  (fail! :oversized-notification))
              text (.toString payload "utf8")
              _ (when-not (.equals payload (.from Buffer text "utf8"))
                  (fail! :invalid-utf8))
              wire (js->clj (js/JSON.parse text) :keywordize-keys true)
              notification {:version (:version wire) :id (:id wire)
                            :source-kind (:sourceKind wire) :source-session-id (:sourceSessionId wire)
                            :producer-id (:producerId wire) :sidecar-locator (:sidecarLocator wire)
                            :queued-at (:queuedAt wire) :file-mtime-ms (.-mtimeMs stat) :path path}]
          (when-not (and (= wire-keys (set (keys wire))) (= 1 (:version wire))
                         (string? (:id wire))
                         (re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" (:id wire))
                         (= (basename path) (str (:id wire) ".json"))
                         (string? (:queuedAt wire))
                         (re-matches #"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z" (:queuedAt wire))
                         (js/Number.isFinite (.parse js/Date (:queuedAt wire)))
                         (= (canonical-locator notification) (:sidecarLocator wire)))
            (fail! :invalid-notification))
          notification)
        (finally (closeSync fd))))))

(defn- scan [options]
  (try
    (let [directory (spool-dir options false)]
      (reduce
       (fn [result name]
         (let [path (join directory name)]
           (try (update result :pending conj (decode! path))
                (catch :default error
                  (update result :rejections conj
                          {:path path :reason (or (:reason (ex-data error)) :malformed-notification)})))))
       {:pending [] :rejections []}
       (sort (filter #(string/ends-with? % ".json") (array-seq (readdirSync directory))))))
    (catch :default error
      (if (= "ENOENT" (.-code error)) {:pending [] :rejections []} (throw error)))))

(defn pending [options]
  (->> (:pending (scan options)) (sort-by (juxt :queued-at :id)) vec))

(defn rejections [options]
  (:rejections (scan options)))

(defn acknowledge! [options notification]
  (let [directory (spool-dir options false)
        path (:path notification)]
    (when-not (and (string? path) (= directory (dirname path))
                   (string/ends-with? (basename path) ".json"))
      (fail! :unsafe-acknowledgement))
    ;; unlink removes the directory entry, never follows a rejected symlink.
    (rmSync path #js {:force true})
    (sync-directory! directory)
    nil))
