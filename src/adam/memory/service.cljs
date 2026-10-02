(ns adam.memory.service
  (:require [adam.knowledge.store :as store]
            [adam.memory.reconcile :as reconcile]
            [adam.replica.identity :as identity]))

(defn drain! [{:keys [store user-uuid] :as options}]
  (reconcile/drain-once!
    (assoc options
      :ensure-source!
      (fn [{:keys [source-kind source-session-id]}]
        (store/ensure-memory-source! store (identity/user-urn user-uuid)
          (identity/session-urn user-uuid source-kind source-session-id)))
      :project-session!
      (fn [{:keys [source-kind source-session-id]}]
        (-> (store/ensure-file-evidence-schema! store)
            (.then (fn [_]
                     (store/reproject-retained-memory! store user-uuid source-kind source-session-id))))))))

(defn drain-after-transcripts!
  "Attempt both queues even if unrelated transcript work fails. Transient
  failure rejects only after healthy groups have progressed, enabling backoff."
  [transcript-drain! memory-drain!]
  (let [failure (atom nil)]
    (-> (.then (js/Promise.resolve nil) (fn [_] (transcript-drain!)))
        (.catch (fn [error] (reset! failure error) {:processed 0 :pending 0}))
        (.then (fn [transcripts]
                 (-> (memory-drain!)
                     (.then (fn [memory]
                              (when (pos? (:failed memory))
                                (reset! failure (js/Error. "Memory notifications remain retryable")))
                              (if-let [error @failure]
                                (js/Promise.reject error)
                                {:processed (+ (or (:processed transcripts) 0)
                                               (:acknowledged memory))
                                 :pending (+ (or (:pending transcripts) 0) (:pending memory))
                                 :memory memory})))))))))
