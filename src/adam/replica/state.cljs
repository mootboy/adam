(ns adam.replica.state
  (:require [clojure.string :as string]
            ["node:crypto" :refer [randomUUID]]
            ["node:fs" :refer [chmodSync closeSync fsyncSync linkSync mkdirSync
                               openSync readFileSync rmSync writeFileSync]]
            ["node:path" :refer [join]]))

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

(defn load-or-create!
  ([agent-dir]
   (load-or-create! agent-dir randomUUID))
  ([agent-dir uuid-fn]
   (let [state-dir (join agent-dir "adam")
         config-path (join state-dir "config.json")]
     (mkdirSync state-dir #js {:recursive true :mode 448})
     (try
       (read-state config-path)
       (catch :default read-error
         (when-not (= "ENOENT" (.-code read-error))
           (throw read-error))
         (let [user-uuid (uuid-fn)
               temp-path (str config-path "." js/process.pid "." (randomUUID) ".tmp")]
           (when-not (valid-user-uuid? user-uuid)
             (throw (js/Error. "generated adam user UUID is empty")))
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
           (read-state config-path)))))))
