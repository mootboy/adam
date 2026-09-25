(ns adam.replica.restore
  (:require [adam.replica.store :as store]
            ["node:crypto" :refer [createHash randomUUID]]
            ["node:fs" :refer [closeSync fsyncSync linkSync openSync rmSync writeFileSync]]
            ["node:path" :refer [basename dirname join]]))

(defn- restoration-error [message]
  (js/Error. message))

(defn- parse-object [raw-json description]
  (let [parsed (try
                 (js/JSON.parse raw-json)
                 (catch :default _
                   (throw (restoration-error (str "invalid " description " JSON")))))]
    (when (or (nil? parsed)
              (not= "object" (goog/typeOf parsed))
              (js/Array.isArray parsed))
      (throw (restoration-error (str description " must be a JSON object"))))
    parsed))

(defn restored-session-filename [session]
  (let [source-file (:source-file session)
        source-name (if source-file (basename source-file) "")]
    (if (.endsWith source-name ".jsonl")
      source-name
      (str (:pi-session-id session) ".jsonl"))))

(defn- validate-header! [restored]
  (let [session (:session restored)
        header (parse-object (:header-json restored) "session header")]
    (when-not (and (= "session" (aget header "type"))
                   (= (:pi-session-id session) (aget header "id")))
      (throw (restoration-error
              "session header identity does not match replica metadata")))))

(defn- validate-entry! [entry expected-ordinal seen-entry-ids]
  (when-not (= expected-ordinal (:ordinal entry))
    (throw (restoration-error (str "invalid entry ordinal " (:ordinal entry)))))
  (when-not (= (:payload-bytes entry)
               (.byteLength js/Buffer (:raw-json entry) "utf8"))
    (throw (restoration-error (str "payload size mismatch for " (:entry-id entry)))))
  (let [actual-hash (-> (createHash "sha256")
                        (.update (:raw-json entry) "utf8")
                        (.digest "hex"))]
    (when-not (= actual-hash (:payload-hash entry))
      (throw (restoration-error (str "payload hash mismatch for " (:entry-id entry))))))
  (let [parsed (parse-object (:raw-json entry) (str "entry " (:entry-id entry)))]
    (when-not (and (= (:entry-id entry) (aget parsed "id"))
                   (= (:parent-id entry) (aget parsed "parentId")))
      (throw (restoration-error (str "entry metadata mismatch for " (:entry-id entry))))))
  (when (and (:parent-id entry)
             (not (contains? seen-entry-ids (:parent-id entry))))
    (throw (restoration-error
            (str "unresolved parent " (:parent-id entry) " for " (:entry-id entry))))))

(defn materialize-session! [replica session-id target-path]
  (-> (store/read-session! replica session-id)
      (.then
       (fn [restored]
         (when-not restored
           (throw (restoration-error "replica session is incomplete or missing")))
         (let [session (:session restored)]
           (when-not (:complete? session)
             (throw (restoration-error "replica session is incomplete or missing")))
           (when (:conflicted? session)
             (throw (restoration-error "replica session is conflicted")))
           (validate-header! restored)
           (let [temporary-path (join (dirname target-path)
                                      (str "." (basename target-path) "."
                                           js/process.pid "." (randomUUID) ".tmp"))
                 file-descriptor (openSync temporary-path "wx" 384)
                 log-hash (createHash "sha256")
                 bytes-written (atom 0)
                 seen-entry-ids (atom #{})
                 entries (:entries restored)
                 entry-count (:entry-count restored)
                 has-final-newline? (:has-final-newline? restored)
                 write! (fn [value]
                          (let [buffer (js/Buffer.from value "utf8")]
                            (writeFileSync file-descriptor buffer)
                            (.update log-hash buffer)
                            (swap! bytes-written + (.-length buffer))))]
             (try
               (write! (str (:header-json restored)
                            (if (or (pos? entry-count) has-final-newline?) "\n" "")))
               (doseq [[expected-ordinal entry] (map-indexed vector entries)]
                 (validate-entry! entry expected-ordinal @seen-entry-ids)
                 (write! (str (:raw-json entry)
                              (if (or (< expected-ordinal (dec entry-count))
                                      has-final-newline?)
                                "\n"
                                "")))
                 (swap! seen-entry-ids conj (:entry-id entry)))
               (when-not (= entry-count (count entries))
                 (throw (restoration-error
                         (str "entry count mismatch: expected " entry-count
                              ", received " (count entries)))))
               (when-not (= (:log-bytes restored) @bytes-written)
                 (throw (restoration-error
                         (str "log size mismatch: expected " (:log-bytes restored)
                              ", received " @bytes-written))))
               (let [actual-log-hash (.digest log-hash "hex")]
                 (when-not (= (:log-hash restored) actual-log-hash)
                   (throw (restoration-error "log hash mismatch"))))
               (fsyncSync file-descriptor)
               (closeSync file-descriptor)
               (try
                 (linkSync temporary-path target-path)
                 (catch :default error
                   (if (= "EEXIST" (.-code error))
                     (throw (restoration-error
                             (str "refusing to overwrite existing session " target-path)))
                     (throw error))))
               {:path target-path
                :current-leaf-id (:current-leaf-id session)}
               (finally
                 (try (closeSync file-descriptor) (catch :default _ nil))
                 (rmSync temporary-path #js {:force true})))))))))
