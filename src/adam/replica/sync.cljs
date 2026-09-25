(ns adam.replica.sync
  (:require [adam.replica.identity :as identity]
            [adam.replica.jsonl :as jsonl]
            [adam.replica.model :as model]
            [adam.replica.store :as store]))

(def default-batch-bytes (* 4 1024 1024))

(defn checkpoint-for-entry [entry]
  {:complete-through-ordinal (:ordinal entry)
   :complete-through-byte-offset (:next-byte-offset entry)
   :committed-prefix-hash (:prefix-hash entry)})

(defn- initial-checkpoint [summary]
  {:complete-through-ordinal -1
   :complete-through-byte-offset (:header-next-byte-offset summary)
   :committed-prefix-hash (:header-prefix-hash summary)})

(defn- prefix-conflict [expected actual]
  {:status :conflict
   :reason :prefix-mismatch
   :expected-hash expected
   :actual-hash actual})

(defn- range-conflict [expected actual]
  {:status :conflict
   :reason :checkpoint-out-of-range
   :expected-offset expected
   :actual-offset actual})

(defn- verify-checkpoint [summary entries checkpoint]
  (let [ordinal (:complete-through-ordinal checkpoint)]
    (cond
      (= -1 ordinal)
      (cond
        (not= (:committed-prefix-hash checkpoint)
              (:header-prefix-hash summary))
        (prefix-conflict (:committed-prefix-hash checkpoint)
                         (:header-prefix-hash summary))

        (not= (:complete-through-byte-offset checkpoint)
              (:header-next-byte-offset summary))
        (range-conflict (:complete-through-byte-offset checkpoint)
                        (:header-next-byte-offset summary))

        :else nil)

      (or (not (integer? ordinal)) (neg? ordinal) (>= ordinal (:entry-count summary)))
      (range-conflict (:complete-through-byte-offset checkpoint) (:log-bytes summary))

      :else
      (if-let [entry (some #(when (= ordinal (:ordinal %)) %) entries)]
        (cond
          (not= (:committed-prefix-hash checkpoint) (:prefix-hash entry))
          (prefix-conflict (:committed-prefix-hash checkpoint) (:prefix-hash entry))

          (not= (:complete-through-byte-offset checkpoint) (:next-byte-offset entry))
          (range-conflict (:complete-through-byte-offset checkpoint)
                          (:next-byte-offset entry))

          :else nil)
        (range-conflict (:complete-through-byte-offset checkpoint) (:log-bytes summary))))))

(defn- make-batch [entries]
  {:entries entries
   :checkpoint (checkpoint-for-entry (peek entries))})

(defn- batch-entries [entries batch-limit]
  (loop [remaining entries
         current []
         current-bytes 0
         batches []]
    (if-let [entry (first remaining)]
      (let [entry-bytes (:payload-bytes entry)]
        (if (and (seq current) (> (+ current-bytes entry-bytes) batch-limit))
          (recur remaining [] 0 (conj batches (make-batch current)))
          (recur (next remaining)
                 (conj current entry)
                 (+ current-bytes entry-bytes)
                 batches)))
      (cond-> batches
        (seq current) (conj (make-batch current))))))

(defn plan-sync
  ([summary entries checkpoint]
   (plan-sync summary entries checkpoint default-batch-bytes))
  ([summary entries checkpoint batch-limit]
   (when-not (and (integer? batch-limit) (pos? batch-limit))
     (throw (js/Error. "batch limit must be a positive integer")))
   (if (and checkpoint
            (= (:log-hash checkpoint) (:log-hash summary))
            (= (:entry-count checkpoint) (:entry-count summary)))
     {:status :unchanged :batches []}
     (let [starting-checkpoint (or checkpoint (initial-checkpoint summary))]
       (if-let [conflict (verify-checkpoint summary entries starting-checkpoint)]
         conflict
         (let [pending (filterv #(> (:ordinal %)
                                    (:complete-through-ordinal starting-checkpoint))
                                entries)
               batches (batch-entries pending batch-limit)
               final-checkpoint (if-let [last-entry (peek entries)]
                                  (checkpoint-for-entry last-entry)
                                  (initial-checkpoint summary))]
           {:status :pending
            :batches batches
            :completion (assoc final-checkpoint
                               :entry-count (:entry-count summary)
                               :log-hash (:log-hash summary))}))))))

(defn- completion-request [session summary checkpoint]
  {:session session
   :checkpoint checkpoint
   :log-bytes (:log-bytes summary)
   :largest-entry-bytes (:largest-entry-bytes summary)
   :has-final-newline? (:has-final-newline? summary)})

(defn- record-conflict! [replica session source-file conflict]
  (let [detail (assoc conflict
                      :session-id (:id session)
                      :source-file source-file)]
    (-> (store/mark-conflict! replica detail)
        (.then (fn [_]
                 {:status :conflict
                  :entries-written 0
                  :batches-written 0
                  :conflict detail})))))

(defn sync-scanned-session!
  [{:keys [summary entries user-uuid source-file current-leaf-id parent identity
           replica batch-bytes]
    :or {batch-bytes default-batch-bytes}
    :as options}]
  (let [session-options (cond-> {:user-uuid user-uuid
                                 :source-file source-file}
                          (contains? options :current-leaf-id)
                          (assoc :current-leaf-id current-leaf-id)
                          parent (assoc :parent parent)
                          identity (assoc :identity identity))
        session (model/session-from-summary summary session-options)]
    (-> (store/get-checkpoint! replica (:id session))
        (.then
         (fn [checkpoint]
           (let [plan (plan-sync summary entries checkpoint batch-bytes)]
             (case (:status plan)
               :unchanged
               (-> (store/complete-session!
                    replica
                    (completion-request session summary checkpoint))
                   (.then (fn [_]
                            {:status :unchanged
                             :entries-written 0
                             :batches-written 0})))

               :conflict
               (record-conflict! replica session source-file (dissoc plan :status))

               :pending
               (let [batches (:batches plan)
                     write-chain
                     (reduce
                      (fn [promise batch]
                        (.then
                         promise
                         (fn [_]
                           (store/write-batch!
                            replica
                            {:session session
                             :entries (mapv #(model/entry-from-scan
                                              user-uuid
                                              (:pi-session-id summary)
                                              %)
                                            (:entries batch))
                             :checkpoint (:checkpoint batch)}))))
                      (js/Promise.resolve nil)
                      batches)]
                 (-> write-chain
                     (.then
                      (fn [_]
                        (store/complete-session!
                         replica
                         (completion-request session summary (:completion plan)))))
                     (.then
                      (fn [_]
                        {:status :mirrored
                         :entries-written (reduce + 0 (map #(count (:entries %)) batches))
                         :batches-written (count batches)}))))))))
        (.catch
         (fn [error]
           (cond
             (store/immutable-entry-conflict? error)
             (record-conflict!
              replica
              session
              source-file
              (assoc (ex-data error) :reason :payload-mismatch))

             (store/checkpoint-conflict? error)
             (record-conflict!
              replica
              session
              source-file
              (assoc (ex-data error) :reason :checkpoint-out-of-range))

             :else
             (js/Promise.reject error)))))))

(defn- resolve-parent [summary user-uuid]
  (when-let [parent-path (:parent-session summary)]
    (try
      (let [parent-header (jsonl/read-session-header parent-path)
            parent-pi-session-id (:pi-session-id parent-header)]
        (when (not= parent-pi-session-id (:pi-session-id summary))
          {:pi-session-id parent-pi-session-id
           :session-id (identity/session-urn user-uuid parent-pi-session-id)}))
      (catch :default _
        nil))))

(defn sync-session-file!
  [{:keys [path user-uuid] :as options}]
  (try
    (let [entries (atom [])
          summary (jsonl/scan-file path {:on-entry #(swap! entries conj %)})
          parent (resolve-parent summary user-uuid)]
      (sync-scanned-session!
       (cond-> (-> options
                   (dissoc :path)
                   (assoc :summary summary
                          :entries @entries
                          :source-file path))
         parent (assoc :parent parent))))
    (catch :default error
      (js/Promise.reject error))))
