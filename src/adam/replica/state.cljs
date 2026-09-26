(ns adam.replica.state
  (:require [clojure.string :as string]
            ["node:crypto" :refer [randomUUID]]
            ["node:fs" :refer [chmodSync closeSync fsyncSync linkSync mkdirSync
                               openSync readFileSync rmSync writeFileSync]]
            ["node:os" :refer [homedir]]
            ["node:path" :as node-path :refer [join]]))

(def ^:private state-version 1)

(defn- valid-user-uuid? [value]
  (and (string? value) (not (string/blank? value))))

(defn- read-state [config-path]
  (let [parsed (js/JSON.parse (readFileSync config-path "utf8"))
        version (aget parsed "version")
        user-uuid (aget parsed "userUuid")]
    (when-not (and (= state-version version) (valid-user-uuid? user-uuid))
      (throw (js/Error. (str "invalid adam state at " config-path))))
    {:version version
     :user-uuid user-uuid
     :path config-path}))

(defn- write-temp-state! [temp-path user-uuid]
  (let [payload (str (js/JSON.stringify
                      #js {:version state-version :userUuid user-uuid}
                      nil
                      2)
                     "\n")
        file-descriptor (openSync temp-path "wx" 384)]
    (try
      (writeFileSync file-descriptor payload "utf8")
      (fsyncSync file-descriptor)
      (finally
        (closeSync file-descriptor)))))

(defn- read-state-if-present [config-path]
  (try
    (read-state config-path)
    (catch :default error
      (if (= "ENOENT" (.-code error))
        nil
        (throw error)))))

(defn- create-state-if-absent! [config-path user-uuid]
  (when-not (valid-user-uuid? user-uuid)
    (throw (js/Error. "generated adam user UUID is empty")))
  (let [state-dir (.dirname node-path config-path)
        temp-path (str config-path "." js/process.pid "." (randomUUID) ".tmp")]
    (mkdirSync state-dir #js {:recursive true :mode 448})
    (try
      (write-temp-state! temp-path user-uuid)
      (try
        (linkSync temp-path config-path)
        (chmodSync config-path 384)
        (catch :default create-error
          (when-not (= "EEXIST" (.-code create-error))
            (throw create-error))))
      (finally
        (rmSync temp-path #js {:force true})))
    (read-state config-path)))

(defn load-or-create!
  ([agent-dir]
   (load-or-create! agent-dir randomUUID))
  ([agent-dir uuid-fn]
   (let [config-path (join agent-dir "adam" "config.json")]
     (or (read-state-if-present config-path)
         (create-state-if-absent! config-path (uuid-fn))))))

(defn- configured-agent-dir []
  (let [configured (aget js/process.env "PI_CODING_AGENT_DIR")]
    (cond
      (and configured (string/starts-with? configured "~/"))
      (.resolve node-path (homedir) (subs configured 2))

      (and configured (not (string/blank? configured))) configured
      :else (join (homedir) ".pi" "agent"))))

(defn- configured-config-home []
  (let [configured (aget js/process.env "XDG_CONFIG_HOME")]
    (if (and configured (not (string/blank? configured)))
      configured
      (join (homedir) ".config"))))

(defn load-or-create-host!
  ([]
   (load-or-create-host! {}))
  ([{:keys [config-home legacy-agent-dir uuid-fn]
     :or {config-home (configured-config-home)
          legacy-agent-dir (configured-agent-dir)
          uuid-fn randomUUID}}]
   (let [config-path (join config-home "adam" "config.json")
         legacy-path (join legacy-agent-dir "adam" "config.json")
         state (read-state-if-present config-path)
         legacy-state (when-not (= config-path legacy-path)
                        (read-state-if-present legacy-path))]
     (when (and state legacy-state
                (not= (:user-uuid state) (:user-uuid legacy-state)))
       (throw (js/Error.
               (str "conflicting adam user identities at " config-path
                    " and " legacy-path))))
     (or state
         (when legacy-state
           (create-state-if-absent! config-path (:user-uuid legacy-state)))
         (create-state-if-absent! config-path (uuid-fn))))))
