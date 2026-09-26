(ns adam.knowledge.migration
  (:require [adam.knowledge.evidence :as evidence]
            [adam.knowledge.store :as store]))

(def target-version evidence/extractor-version)

(defn rebuild-if-needed!
  [{:keys [store user-id session-files rebuild-session!]}]
  (-> (store/code-memory-version! store user-id)
      (.then
       (fn [current-version]
         (if (>= (or current-version 0) target-version)
           {:status :current :version target-version}
           (let [paths (->> (if (fn? session-files)
                              (session-files)
                              session-files)
                            distinct sort vec)
                 rebuild
                 (reduce
                  (fn [promise path]
                    (.then promise (fn [_] (rebuild-session! path))))
                  (js/Promise.resolve nil)
                  paths)]
             (-> rebuild
                 (.then
                  (fn [_]
                    (store/complete-code-memory-rebuild!
                     store user-id target-version)))
                 (.then
                  (fn [_]
                    {:status :rebuilt
                     :version target-version
                     :session-count (count paths)})))))))))
