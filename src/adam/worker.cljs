(ns adam.worker
  (:require [adam.replica.config :as config]
            [adam.sources.claude-code.runtime :as runtime]
            [adam.sources.claude-code.worker :as worker]))

(defn start []
  (let [resolved-config (config/resolve-process-config)]
    (when-not (:enabled? resolved-config)
      (throw (js/Error. (str "adam worker unavailable: " (:reason resolved-config)))))
    (let [{:keys [process! close!]} (runtime/create-runtime resolved-config)]
      (-> (worker/run-worker! {:inbox-options {}
                               :process! process!})
          (.finally close!)))))
