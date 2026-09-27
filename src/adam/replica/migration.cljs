(ns adam.replica.migration
  (:require [adam.replica.store :as store]))

(def target-version 2)

(defn migrate-if-needed! [{:keys [store user-id]}]
  (-> (store/session-identity-version! store user-id)
      (.then
       (fn [current-version]
         (if (>= (or current-version 0) target-version)
           {:status :current :version target-version}
           (-> (store/migrate-pi-session-identities!
                store user-id target-version)
               (.then
                (fn [_]
                  {:status :migrated :version target-version}))))))))
