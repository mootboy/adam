(ns adam.worker
  (:require [adam.replica.config :as config]
            [adam.memory.inbox :as memory-inbox]
            [adam.memory.service :as memory-service]
            [adam.sources.claude-code.inbox :as transcript-inbox]
            [adam.sources.claude-code.runtime :as runtime]
            [adam.sources.claude-code.worker :as worker]))

(defn start []
  (let [resolved-config (config/resolve-process-config)]
    (when-not (:enabled? resolved-config)
      (throw (js/Error. (str "adam worker unavailable: " (:reason resolved-config)))))
    (let [{:keys [process! drain-memory! close!]} (runtime/create-runtime resolved-config)]
      (-> ((if (some #{"--once"} (array-seq (.-argv js/process))) worker/run-once! worker/run-worker!)
            {:inbox-options {}
             :drain! #(memory-service/drain-after-transcripts!
                        (fn [] (worker/drain-once! {:inbox-options {} :process! process!}))
                        drain-memory!)
             :pending? #(or (seq (transcript-inbox/pending {}))
                            (seq (memory-inbox/pending {})))})
          (.then (fn [result]
                   (when (and (= :busy (:status result))
                              (some #{"--once"} (array-seq (.-argv js/process))))
                     (throw (js/Error. "Adam reconciliation is already running; retry --once later")))
                   result))
          (.finally close!)))))
