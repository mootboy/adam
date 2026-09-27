(ns adam.sources.claude-code.sync
  (:require [adam.replica.sync :as replica-sync]
            [adam.sources.claude-code.model :as model]
            [adam.sources.claude-code.store :as store]))

(defn- completion-request [session stream summary checkpoint]
  {:session session
   :stream stream
   :checkpoint checkpoint
   :log-bytes (:log-bytes summary)
   :source-bytes (:source-bytes summary)
   :incomplete-tail-bytes (:incomplete-tail-bytes summary)
   :largest-entry-bytes (:largest-entry-bytes summary)
   :has-final-newline? (:has-final-newline? summary)})

(defn- sync-stream!
  [{:keys [store session stream summary entries batch-bytes]}]
  (-> (store/get-stream-checkpoint! store (:id stream))
      (.then
       (fn [checkpoint]
         (let [plan (replica-sync/plan-sync summary entries checkpoint batch-bytes)]
           (case (:status plan)
             :conflict
             (let [conflict (assoc (dissoc plan :status)
                                   :session-id (:id session)
                                   :stream-id (:id stream)
                                   :source-file (:path summary))]
               (-> (store/mark-stream-conflict! store conflict)
                   (.then (fn [_] {:status :conflict
                                   :entries-written 0
                                   :batches-written 0
                                   :conflict conflict}))))

             :unchanged
             (-> (store/complete-stream!
                  store (completion-request session stream summary checkpoint))
                 (.then (fn [_] {:status :unchanged
                                 :entries-written 0
                                 :batches-written 0})))

             :pending
             (let [batches (:batches plan)]
               (-> (reduce
                    (fn [promise batch]
                      (.then promise
                             (fn [_]
                               (store/write-stream-batch!
                                store
                                {:session session
                                 :stream stream
                                 :entries (:entries batch)
                                 :checkpoint (:checkpoint batch)}))))
                    (js/Promise.resolve nil)
                    batches)
                   (.then
                    (fn [_]
                      (store/complete-stream!
                       store
                       (completion-request session stream summary
                                           (:completion plan)))))
                   (.then
                    (fn [_]
                      {:status :mirrored
                       :entries-written (reduce + 0 (map #(count (:entries %)) batches))
                       :batches-written (count batches)}))))))))))

(defn sync-session-scan!
  [{:keys [store user-uuid scan batch-bytes]
    :or {batch-bytes replica-sync/default-batch-bytes}}]
  (let [{:keys [session streams entries]} (model/from-scan user-uuid scan)
        entries-by-stream (group-by :stream-id entries)
        summaries-by-id (into {} (map (juxt :stream-id identity) (:streams scan)))]
    (-> (store/ensure-claude-schema! store)
        (.then
         (fn [_]
           (reduce
            (fn [promise stream]
              (.then
               promise
               (fn [results]
                 (-> (sync-stream!
                      {:store store
                       :session session
                       :stream stream
                       :summary (get summaries-by-id (:stream-id stream))
                       :entries (get entries-by-stream (:stream-id stream) [])
                       :batch-bytes batch-bytes})
                     (.then (fn [result] (conj results result)))))))
            (js/Promise.resolve [])
            streams)))
        (.then
         (fn [results]
           (if-let [conflict (some #(when (= :conflict (:status %)) %) results)]
             conflict
             (-> (store/complete-claude-session! store {:session session})
                 (.then
                  (fn [_]
                    {:status (if (every? #(= :unchanged (:status %)) results)
                               :unchanged
                               :mirrored)
                     :entries-written (reduce + 0 (map :entries-written results))
                     :batches-written (reduce + 0 (map :batches-written results))
                     :streams (count streams)})))))))))
