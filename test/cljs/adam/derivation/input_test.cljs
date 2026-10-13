(ns adam.derivation.input-test
  (:require [adam.derivation.config :as config]
            [adam.derivation.core :as core]
            [adam.derivation.store :as store]
            [adam.derivation.publication-test :as publication]
            [adam.derivation.store-test :refer [reader]]
            [cljs.test :refer-macros [deftest is async]]))

(defn- fixture
  ([memory-rows] (fixture memory-rows {}))
  ([memory-rows {:keys [prior-rows dependency-rows]}]
  (let [calls (atom []) closes (atom 0)
        tx #js {:run (fn [query params]
                      (swap! calls conj {:query query :params params})
                      (js/Promise.resolve
                        #js {:records (to-array
                           (map (fn [row] #js {:get (fn [key] (get row key))})
                             (cond
                               (re-find #"RETURN u.id AS userId" query) [{"userId" reader}]
                               (re-find #"AS inputMemory" query) memory-rows
                               (re-find #"AS parents" query) prior-rows
                               (re-find #"AS ownerIds" query) dependency-rows
                               :else [])))}))}]
    {:driver #js {:session (fn [_] #js {:executeRead (fn [work] (work tx))
                                      :close (fn [] (swap! closes inc) (js/Promise.resolve))})}
     :calls calls :closes closes})))

(deftest authorized-input-snapshots-are-complete-and-closed
  (async done
    (let [row {"ownerIds" [reader] "sessionIds" ["urn:adam:session:reader:pi:s"]
               "sourceKind" "pi" "sourceSessionId" "s"
               "inputMemory" #js {:id "memory-a" :producer "synthetic" :content "Prefer simple boundaries"}
               "supportRecords" [] "kind" "observation" "sourceEntries" #js [#js {:streamId "main" :entryId "entry-a"}]
               "repositoryOrigins" ["example.com/repo"] "repositoryIds" ["repo-id"]
               "linkedPaths" ["src/a.cljs"]}
          cases [{:rows [row] :ids ["memory-a"]}
                 {:rows [] :ids []}
                 {:rows [(assoc row "inputMemory" #js {:id "memory-a" :producer "synthetic" :content "Dropped support" :dropped true})]
                  :ids ["memory-a"] :dropped? true}
                 {:rows [(assoc row "kind" "reflection" "supportRecords" [#js {:id "actual-support-id" :memoryId "support" :producer "synthetic" :sessionIds #js ["urn:adam:session:reader:pi:s"]}] "inputMemory" #js {:id "reflection-a" :producer "synthetic" :content "Scoped synthesis" :supportingObservationIds #js ["support"]})]
                  :ids ["reflection-a"] :supports ["actual-support-id"]}
                 {:rows [(assoc row "kind" "reflection" "supportRecords" [#js {:id "actual-support-id" :memoryId "support" :producer "synthetic" :sessionIds #js ["urn:adam:session:reader:pi:s"]}]
                                "inputMemory" #js {:id "reflection-a" :producer "synthetic" :content "Scoped synthesis" :dropped true :supportingObservationIds #js ["support"]})]
                  :ids ["reflection-a"] :supports ["actual-support-id"] :ignore-drop? true}
                 {:rows [(assoc row "kind" "reflection")] :error :unresolved-evidence}
                 {:rows [(assoc row "ownerIds" ["not-a-user-urn"])] :error :unresolved-evidence}
                 {:rows [(assoc row "sourceSessionId" "different")] :error :unresolved-evidence}
                 {:rows [(assoc row "kind" nil)] :error :unresolved-evidence}
                 {:rows [(assoc row "ownerIds" [reader "other"])] :error :unresolved-evidence}]
          attempt (fn [{:keys [rows ids error supports dropped? ignore-drop?]}]
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
                                   (when dropped? (is (get-in snapshot [:memories 0 :dropped?])))
                                   (when ignore-drop? (is (false? (get-in snapshot [:memories 0 :dropped?]))))))
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

(deftest configured-refresh-wires-directly-to-the-graph-loader-for-every-selector
  (async done
    (let [recipes (config/parse-config "derivations:\n  preferences:\n    version: 1\n    guidance: Preserve scope.\n")
          row {"ownerIds" [reader] "sessionIds" ["urn:adam:session:reader:pi:s"]
               "sourceKind" "pi" "sourceSessionId" "s" "kind" "observation"
               "inputMemory" #js {:id "memory-a" :producer "synthetic" :content "Prefer simple boundaries"}
               "sourceEntries" [] "repositoryOrigins" ["example.com/repo"] "linkedPaths" ["src/a.cljs"]}
          requests [{:name "preferences"} {:name "preferences" :repository "git@example.com:repo.git"}
                    {:name "preferences" :session "urn:adam:session:reader:pi:s"}
                    {:name "preferences" :language "cljs"}]
          attempt (fn [request]
                    (let [{:keys [driver]} (fixture [row])
                          db (store/with-driver driver "neo4j" reader)
                          deps {:load-input! (fn [request]
                                              (-> (store/load-input! db request)
                                                  (.then #(assoc % :model {:provider "fake" :id "model" :context-window 20000 :max-output-tokens 8192}))))
                                :measure-input (constantly 100)
                                :execute! (fn [request]
                                            (is (= ["memory-a"] (vec (aget (js/JSON.parse (:payload request)) "candidate-memory-ids"))))
                                            {:finish-reason :stop :conclusions [] :links []})}]
                      (-> (core/refresh-configured! deps recipes request)
                          (.then (fn [outcomes] (is (= :generated (:status (first outcomes)))))))))]
      (-> (js/Promise.all (to-array (map attempt requests)))
          (.then (fn [_] (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest revoked-prior-dependencies-withhold-all-comparison-text
  (async done
    (-> (publication/generated-run)
        (.then (fn [run]
                 (let [{:keys [driver]} (fixture []
                                         {:prior-rows [(publication/run-row run)]
                                          :dependency-rows [{"memoryId" "memory-a" "ownerIds" ["urn:adam:user:other"]
                                                             "sessionIds" ["other-session"] "kind" "observation"
                                                             "producer" "synthetic" "content" "PRIVATE"
                                                             "permitted" false "linked" true}]})]
                   (store/load-input! (store/with-driver driver "neo4j" reader) {:name "preferences"}))))
        (.then (fn [snapshot]
                 (is (= {:id "unavailable"} (:comparison snapshot)))
                 (is (= [] (:memories snapshot)))
                 (is (not (re-find #"PRIVATE" (pr-str snapshot))))))
        (.catch (fn [error] (is false (.-stack error))))
        (.finally done))))

(deftest malformed-source-outcomes-retain-content-free-diagnostic-counts
  (async done
    (let [{:keys [driver]} (fixture [{"ownerIds" [reader "other"] "sessionIds" [] "inputMemory" nil}])
          deps {:load-input! #(store/load-input! (store/with-driver driver "neo4j" reader) %)
                :execute! (fn [_] (is false "malformed evidence must never execute"))}
          recipes (config/parse-config "derivations:\n  preferences:\n    version: 1\n    guidance: Preserve scope.\n")]
      (-> (core/refresh-configured! deps recipes {:name "preferences"})
          (.then (fn [outcomes]
                   (is (= [{:name "preferences" :status :failed :code :unresolved-evidence :invalid-source-count 1}] outcomes))))
          (.catch (fn [error] (is false (.-stack error))))
          (.finally done)))))
