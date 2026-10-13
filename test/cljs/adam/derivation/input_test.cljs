(ns adam.derivation.input-test
  (:require [adam.derivation.config :as config]
            [adam.derivation.store :as store]
            [adam.derivation.store-test :refer [reader]]
            [cljs.test :refer-macros [deftest is async]]))

(defn- fixture [memory-rows]
  (let [calls (atom []) closes (atom 0)
        tx #js {:run (fn [query params]
                      (swap! calls conj {:query query :params params})
                      (js/Promise.resolve
                        #js {:records (to-array
                           (map (fn [row] #js {:get (fn [key] (get row key))})
                             (cond
                               (re-find #"RETURN u.id AS userId" query) [{"userId" reader}]
                               (re-find #"AS inputMemory" query) memory-rows
                               :else [])))}))}]
    {:driver #js {:session (fn [_] #js {:executeRead (fn [work] (work tx))
                                      :close (fn [] (swap! closes inc) (js/Promise.resolve))})}
     :calls calls :closes closes}))

(deftest authorized-input-snapshots-are-complete-and-closed
  (async done
    (let [row {"ownerIds" [reader] "sessionIds" ["urn:adam:session:reader:pi:s"]
               "sourceKind" "pi" "sourceSessionId" "s"
               "inputMemory" #js {:id "memory-a" :producer "synthetic" :content "Prefer simple boundaries"}
               "kind" "observation" "sourceEntries" #js [#js {:streamId "main" :entryId "entry-a"}]
               "repositoryOrigins" ["example.com/repo"] "repositoryIds" ["repo-id"]
               "linkedPaths" ["src/a.cljs"]}
          cases [{:rows [row] :ids ["memory-a"]}
                 {:rows [] :ids []}
                 {:rows [(assoc row "inputMemory" #js {:id "memory-a" :producer "synthetic" :content "Dropped support" :dropped true})]
                  :ids ["memory-a"] :dropped? true}
                 {:rows [(assoc row "kind" "reflection" "inputMemory" #js {:id "reflection-a" :producer "synthetic" :content "Scoped synthesis" :supportingObservationIds #js ["support"]})]
                  :ids ["reflection-a"] :supports [(str "urn:adam:observation:" (subs reader 14) ":pi:s:synthetic:support")]}
                 {:rows [(assoc row "sourceSessionId" "different")] :error :unresolved-evidence}
                 {:rows [(assoc row "kind" nil)] :error :unresolved-evidence}
                 {:rows [(assoc row "ownerIds" [reader "other"])] :error :unresolved-evidence}]
          attempt (fn [{:keys [rows ids error supports dropped?]}]
                    (let [{:keys [driver closes]} (fixture rows)]
                      (-> (store/load-input! (store/with-driver driver "neo4j" reader) {:name "preferences"})
                          (.then (fn [snapshot]
                                   (is (nil? error))
                                   (is (= reader (:reader-id snapshot)))
                                   (is (= ids (mapv :id (:memories snapshot))))
                                   (when (seq ids)
                                     (is (= [{:stream-id "main" :entry-id "entry-a"}]
                                            (get-in snapshot [:memories 0 :source-entries])))
                                     (is (= [] (get-in snapshot [:memories 0 :source-occasions]))))
                                   (when supports (is (= supports (get-in snapshot [:memories 0 :supporting-memory-ids]))))
                                   (when dropped? (is (get-in snapshot [:memories 0 :dropped?])))))
                          (.catch (fn [failure] (is (= error (config/error-code failure)))))
                          (.finally (fn [] (is (= 1 @closes)))))))]
      (-> (js/Promise.all (to-array (map attempt cases)))
          (.then (fn [_] (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest invalid-input-selectors-fail-before-driver-access
  (async done
    (let [opens (atom 0) db (store/with-driver #js {:session (fn [_] (swap! opens inc))} "neo4j" reader)]
      (-> (store/load-input! db {:name "preferences" :repository "shorthand"})
          (.then (fn [_] (is false "invalid selector must fail")))
          (.catch (fn [error] (is (= :invalid-selector (config/error-code error))) (is (zero? @opens))))
          (.finally done)))))
