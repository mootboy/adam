(ns adam.derivation.publication-test
  (:require [adam.derivation.config :as config]
            [adam.derivation.core :as core]
            [adam.derivation.store :as store]
            [adam.derivation.store-test :refer [reader recording-driver]]
            [cljs.spec.alpha :as s]
            [cljs.test :refer [async deftest is]]))

(def recipe (get (config/parse-config "derivations:\n  preferences:\n    version: 1\n    guidance: Preserve scope.\n") "preferences"))
(def input {:reader-id reader :name "preferences" :filter {:type :none} :recipe recipe
            :model {:provider "fake" :id "model" :context-window 20000 :max-output-tokens 8192}
            :grants [] :memories [{:id "memory-a" :owner-id reader :session-id "session-a"
                                  :producer "synthetic" :kind :observation :content "Prefer simplicity"}]})
(defn generated-run []
  (-> (core/refresh! {:measure-input (constantly 100)
                      :execute! (constantly {:finish-reason :stop :conclusions [] :links []})} input)
      (.then (fn [result] (:run result)))))

(deftest publication-preflight-is-closed-before-io
  (async done
    (-> (generated-run)
        (.then (fn [run]
                 (let [_ (is (s/valid? ::store/publication run))
                       opens (atom 0) db (store/with-driver #js {:session #(swap! opens inc)} "neo4j" reader)
                       invalid [(assoc run :derivation-id "different") (assoc run :api-key "must-not-store")
                                (assoc run :id "not-a-run")
                                (assoc run :input-memory-ids ["memory-a" "memory-a"])
                                (assoc run :dependency-fingerprints {})
                                (assoc run :configuration-snapshot-json "{\"password\":\"must-not-store\"}")]]
                   (-> (js/Promise.all (to-array (map #(-> (store/publish-refresh! db {:name "preferences"} "token" %) (.catch config/error-code)) invalid)))
                       (.then (fn [codes]
                                (is (= (repeat (count invalid) :invalid-output) (array-seq codes)))
                                (is (zero? @opens))))))))
        (.then (fn [_] (done)))
        (.catch (fn [error] (is false (.-stack error)) (done))))))

(defn run-row [run]
  {"derivationId" (:derivation-id run) "name" "preferences" "filterType" "none" "filterValue" nil
   "runId" (:id run) "runJson" (core/snapshot-json run) "memoryIds" (:input-memory-ids run)
   "parents" [(:derivation-id run)] "conclusions" [] "dependenciesLinked" true})

(defn- read-case! [run cause]
  (let [writes (atom [])
        {:keys [driver calls]} (recording-driver
          (fn [query _]
            (cond
              (re-find #"AS parents" query)
              [(cond-> (run-row run)
                 (= cause :wrong-parent) (assoc "parents" ["different-derivation"]) )]
              (re-find #"AS ownerIds" query)
              [{"memoryId" "memory-a" "ownerIds" (if (= cause :ambiguous-owner) [reader "other"] [reader])
                "sessionIds" (if (= cause :unresolved-evidence) [] ["session-a"])
                "kind" "observation" "producer" "synthetic" "content" "Prefer simplicity"
                "permitted" (not= cause :unauthorized) "linked" (not= cause :unlinked)}]
              (re-find #"DELETE old" query) (do (swap! writes conj query) [{"id" (:id run)}])
              :else [])))
        db (store/with-driver driver "neo4j" reader)]
    (-> (store/read-runs! db {:name "preferences"})
        (.then (fn [{:keys [items]}]
                 (is (= (if (contains? #{:unlinked :ambiguous-owner :wrong-parent} cause) :unresolved-evidence cause) (:status (first items))))
                 (is (= (= cause :available) (contains? (first items) :run)))
                 (is (empty? @writes))
                 (store/repair-dependencies! db {:name "preferences"})))
        (.then (fn [{:keys [items]}]
                 (is (= (cond (contains? #{:available :unlinked} cause) :repaired
                           (contains? #{:ambiguous-owner :wrong-parent} cause) :unresolved-evidence
                           :else cause) (:status (first items))))
                 (is (= (contains? #{:available :unlinked} cause) (boolean (seq @writes))))
                 (is (nil? (:filterType (:params (first (filter #(re-find #"AS parents" (:query %)) @calls)))))))))))

(deftest bounded-read-and-repair-preserve-run-wide-permission-requirements
  (async done
    (-> (generated-run)
        (.then (fn [run]
                 (js/Promise.all (to-array (map #(read-case! run %) [:available :unauthorized :unresolved-evidence :unlinked :ambiguous-owner :wrong-parent])))))
        (.then (fn [_] (done)))
        (.catch (fn [error] (is false (.-stack error)) (done))))))

(deftest failed-or-fenced-publication-never-writes-run-content
  (async done
    (-> (generated-run)
        (.then (fn [run]
                 (js/Promise.all
                   (to-array
                     (for [cause [:fenced :unauthorized :unresolved-evidence]]
                       (let [{:keys [driver calls closes]}
                             (recording-driver
                               (fn [query _]
                                 (cond
                                   (re-find #"AS leaseValid" query) (if (= cause :fenced) [] [{"leaseValid" true}])
                                   (re-find #"AS ownerIds" query)
                                   [{"memoryId" "memory-a" "ownerIds" [reader] "sessionIds" (if (= cause :unresolved-evidence) [] ["session-a"])
                                     "kind" "observation" "producer" "synthetic" "content" "Prefer simplicity"
                                     "permitted" (not= cause :unauthorized) "linked" (not= cause :unresolved-evidence)}]
                                   :else [])))
                             db (store/with-driver driver "neo4j" reader)]
                         (-> (store/publish-refresh! db {:name "preferences"} "token" run)
                             (.catch #(hash-map :status (config/error-code %)))
                             (.then (fn [result]
                                      (is (= cause (:status result)))
                                      (is (not-any? #(re-find #"CREATE \(r:AdamDerivationRun" (:query %)) @calls))
                                      (is (= 1 @closes)))))))))))
        (.then (fn [_] (done)))
        (.catch (fn [error] (is false (.-stack error)) (done))))))
