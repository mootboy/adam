(ns adam.replica.config
  (:require [clojure.string :as string]
            [cljs.spec.alpha :as s]
            ["node:buffer" :refer [Buffer]]
            ["node:crypto" :refer [randomUUID]]
            ["node:fs" :as fs]
            ["node:os" :refer [homedir]]
            ["node:net" :refer [isIP]]
            ["yaml" :as yaml]
            ["node:path" :as path]))

(def environment-keys
  ["ADAM_NEO4J_URI"
   "ADAM_NEO4J_USERNAME"
   "ADAM_NEO4J_PASSWORD"
   "ADAM_NEO4J_DATABASE"])

(def ^:private required-keys
  ["ADAM_NEO4J_URI" "ADAM_NEO4J_USERNAME" "ADAM_NEO4J_PASSWORD"])

(def ^:private missing-config-reason
  "ADAM_NEO4J_URI, ADAM_NEO4J_USERNAME, and ADAM_NEO4J_PASSWORD are required")

(defn- present? [value]
  (and (string? value) (not (string/blank? value))))

(def ^:private encrypted-schemes
  #{"bolt+s" "bolt+ssc" "neo4j+s" "neo4j+ssc"})

(def ^:private supported-schemes
  (into encrypted-schemes ["bolt" "neo4j"]))

(defn- loopback-host? [host]
  (let [normalized (string/lower-case host)]
    (or (= normalized "localhost")
        (= normalized "::1")
        (= normalized "[::1]")
        (and (= 4 (isIP normalized)) (string/starts-with? normalized "127.")))))

(defn- uri-error [uri]
  (try
    (let [url (js/URL. uri)
          scheme (string/replace (.-protocol url) #":$" "")]
      (cond
        (or (not (contains? supported-schemes scheme))
            (seq (.-username url)) (seq (.-password url)))
        "ADAM_NEO4J_URI must be a valid bolt or neo4j URI"

        (= "neo4j" scheme)
        "routed Neo4j connections require TLS; use neo4j+s or neo4j+ssc, or direct bolt on loopback"

        (and (not (contains? encrypted-schemes scheme))
             (not (loopback-host? (.-hostname url))))
        "unencrypted Neo4j transport is allowed only for loopback hosts"

        :else nil))
    (catch :default _
      "ADAM_NEO4J_URI must be a valid bolt or neo4j URI")))

(defn process-environment []
  (into {}
        (map (fn [key] [key (aget js/process.env key)]))
        environment-keys))

(defn resolve-config [environment]
  (if-not (every? #(present? (get environment %)) required-keys)
    {:enabled? false
     :reason missing-config-reason}
    (let [config {:enabled? true
                  :uri (string/trim (get environment "ADAM_NEO4J_URI"))
                  :username (string/trim (get environment "ADAM_NEO4J_USERNAME"))
                  :password (get environment "ADAM_NEO4J_PASSWORD")
                  :database (let [database (get environment "ADAM_NEO4J_DATABASE")]
                              (if (present? database) (string/trim database) "neo4j"))}]
      (if-let [reason (uri-error (:uri config))]
        {:enabled? false
         :reason reason}
        config))))

;; Connection configuration is intentionally separate from identity and recipes.
;; Never attach source values or spec explain-data to diagnostics.
(defn- reject! [message]
  (throw (js/Error. message)))

(defn- bounded-text [limit]
  (s/and string? #(not (string/blank? %)) #(<= (.byteLength Buffer % "utf8") limit)
         #(not (re-find #"[\u0000-\u001f\u007f-\u009f]" %))))

(s/def ::version #{1})
(s/def ::uri (s/and (bounded-text 4096) #(nil? (uri-error (string/trim %)))))
(s/def ::username (bounded-text 4096))
(s/def ::password (bounded-text 8192))
(s/def ::database (bounded-text 4096))
(s/def ::connection
  (s/and (s/keys :req-un [::version ::uri ::username ::password] :opt-un [::database])
         #(every? #{:version :uri :username :password :database} (keys %))))

(defn normalize-connection [settings]
  (when-not (s/valid? ::connection settings)
    (reject! "Invalid Adam connection settings; check version, URI, username, password and database"))
  (merge settings {:uri (string/trim (:uri settings))
                   :username (string/trim (:username settings))
                   :database (string/trim (get settings :database "neo4j"))}))

(defn configured-config-home []
  (let [root (aget js/process.env "XDG_CONFIG_HOME")]
    (if (string/blank? root) (.join path (homedir) ".config") root)))

(defn connection-path
  ([] (connection-path (configured-config-home)))
  ([root]
   (when-not (and (string? root) (.isAbsolute path root))
     (reject! "Adam configuration root must be absolute"))
   (.join path root "adam" "connection.yaml")))

(defn- safe-stat! [stat directory? private?]
  (when-not (and (contains? #{"linux" "darwin"} js/process.platform)
                 (= (.-uid stat) (.getuid js/process))
                 (if directory? (.isDirectory stat) (.isFile stat))
                 (zero? (bit-and (.-mode stat) (if private? 63 18)))
                 (or directory? (= 1 (aget stat "nlink"))))
    (reject! "Unsafe Adam connection configuration; require an owned non-symlink directory (0700) and file (0600)")))

(defn- check-directories! [root]
  ;; The XDG root may be world-readable, but must not be writable by others.
  ;; The Adam directory contains secrets and must be owner-only.
  (safe-stat! (.lstatSync fs root) true false)
  (safe-stat! (.lstatSync fs (.join path root "adam")) true true))

(defn read-connection!
  ([] (read-connection! (configured-config-home)))
  ([root]
   (let [filename (connection-path root)]
     (try
       (check-directories! root)
       (when-not (number? (.-O_NOFOLLOW (.-constants fs)))
         (reject! "Safe Adam connection reads are unsupported on this platform"))
       (let [fd (.openSync fs filename (bit-or (.-O_RDONLY (.-constants fs))
                                             (.-O_NOFOLLOW (.-constants fs))
                                             (.-O_NONBLOCK (.-constants fs))))]
         (try
           (safe-stat! (.fstatSync fs fd) false true)
           (let [buffer (.alloc Buffer 65537)
                 size (loop [offset 0]
                        (if (= offset 65537) offset
                          (let [n (.readSync fs fd buffer offset (- 65537 offset) offset)]
                            (if (zero? n) offset (recur (+ offset n))))))]
             (when (> size 65536) (reject! "Adam connection configuration exceeds 64 KiB"))
             (let [source (.decode (js/TextDecoder. "utf-8" #js {:fatal true}) (.subarray buffer 0 size))
                   document (.parseDocument yaml source #js {:version "1.2" :uniqueKeys true :merge false})]
               (when (or (seq (.-errors document)) (seq (.-warnings document)))
                 (reject! "Invalid Adam connection YAML"))
               (normalize-connection
                (js->clj (.toJS document #js {:maxAliasCount 0}) :keywordize-keys true))))
           (finally (.closeSync fs fd))))
       (catch :default error
         (if (= "ENOENT" (.-code error)) nil
           (reject! "Cannot read Adam connection.yaml; check YAML settings, ownership, permissions (directory 0700/file 0600) and non-symlink paths")))))))

(defn save-connection!
  ([settings] (save-connection! settings (configured-config-home)))
  ([settings root]
   (let [normalized (normalize-connection settings)
         filename (connection-path root)
         directory (.dirname path filename)
         temporary (.join path directory (str ".connection-" (randomUUID) ".tmp"))]
     (try
       (.mkdirSync fs directory #js {:recursive true :mode 448})
       (check-directories! root)
       ;; Validate any existing destination before replacing it. Invalid or unsafe
       ;; files need explicit operator repair, not implicit credential overwrite.
       (read-connection! root)
       (let [fd (.openSync fs temporary "wx" 384)]
         (try
           (.writeFileSync fs fd (.stringify yaml (clj->js normalized) #js {:version "1.2" :lineWidth 0}) "utf8")
           (.fsyncSync fs fd)
           (finally (.closeSync fs fd))))
       (.renameSync fs temporary filename)
       nil
       (catch :default _
         (reject! "Cannot save Adam connection.yaml; check settings, ownership, private permissions and non-symlink paths"))
       (finally
         (try (.rmSync fs temporary #js {:force true}) (catch :default _ nil)))))))

(defn environment-configured? [environment]
  ;; Even an explicit empty/partial environment disables file fallback. Tests
  ;; and automation must never inherit the remainder of a saved connection.
  (boolean (some #(some? (get environment %)) environment-keys)))

(defn resolve-process-config
  ([] (resolve-process-config (process-environment) (configured-config-home)))
  ([environment root]
   (if (environment-configured? environment)
     (resolve-config environment)
     (try
       (if-let [settings (read-connection! root)]
         (assoc (dissoc settings :version) :enabled? true)
         {:enabled? false :reason (str missing-config-reason "; alternatively run /adam:configure to save a private connection.yaml")})
       (catch :default error
         {:enabled? false :reason (.-message error)})))))
