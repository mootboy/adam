(ns adam.knowledge.memory-migration
  (:require [adam.knowledge.store :as store]))

(def target-version 1)

(defn migrate-if-needed! [{:keys [store user-id]}]
  (-> (store/memory-identity-version! store user-id)
      (.then
       (fn [current-version]
         (if (>= (or current-version 0) target-version)
           {:status :current :version target-version}
           (-> (store/migrate-memory-identities!
                store user-id target-version)
               (.then
                (fn [_]
                  {:status :migrated :version target-version}))))))))
