(ns adam.derivation.core-test
  (:require [adam.derivation.config :as config]
            [adam.derivation.core :as core]
            [adam.derivation.evidence-test :as fixtures]
            [cljs.test :refer [async deftest is]]))

(def recipe (get (config/parse-config "derivations:\n  preferences:\n    version: 1\n    guidance: Preserve scope.\n") "preferences"))
(def model {:provider "fake" :id "model-1" :context-window 20000 :max-output-tokens 8192})
(def input {:reader-id fixtures/own :grants [] :name "preferences" :filter {:type :none}
            :recipe recipe :model model :memories [(fixtures/observation "a" fixtures/own "user-1")]})
(def conclusion {:id "c1" :content "Prefer simple boundaries" :category "architecture"
                 :basis :explicit :scope-type :personal :scope-subject-ids [fixtures/own]
                 :supporting-memory-ids ["a"] :conflicting-memory-ids []})

(deftest response-json-is-closed-and-finish-status-is-not-model-controlled
  (let [json (js/JSON.stringify (clj->js {:conclusions [conclusion] :links []}))]
    (is (= :explicit (:basis (first (:conclusions (core/decode-response json :stop)))))))
  (doseq [json ["not json" "{\"conclusions\":[],\"links\":[],\"finish-reason\":\"stop\"}"
               "{\"conclusions\":{},\"links\":[]}"]]
    (try (core/decode-response json :stop) (is false "must reject invalid model JSON")
         (catch :default error (is (= :invalid-output (config/error-code error)))))))

(deftest one-fake-call-produces-a-complete-provider-bearing-run
  (async done
    (let [calls (atom [])]
      (-> (core/refresh! {:measure-input (constantly 200)
                          :execute! (fn [request]
                                      (swap! calls conj request)
                                      {:finish-reason :stop :conclusions [conclusion] :links []})}
                         input)
          (.then (fn [result]
                   (is (= :generated (:status result)))
                   (is (= "fake" (get-in result [:run :provider])))
                   (is (= "model-1" (get-in result [:run :model-id])))
                   (is (= ["a"] (get-in result [:run :input-memory-ids])))
                   (is (= 1 (count @calls)))
                   (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest oversized-input-and-wrong-model-make-no-executor-call
  (async done
    (let [calls (atom 0) deps {:measure-input (constantly 30000)
                              :execute! (fn [_] (swap! calls inc))}
          wrong-model (assoc-in input [:recipe :model] {:provider "different" :id "different"})]
      (-> (js/Promise.all
            #js [(-> (core/refresh! deps input) (.catch config/error-code))
                 (-> (core/refresh! (assoc deps :measure-input (constantly 200)) wrong-model)
                     (.catch config/error-code))])
          (.then (fn [codes]
                   (is (= [:input-too-large :invalid-model] (vec (array-seq codes))))
                   (is (zero? @calls)) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest valid-cache-is-reused-and-session-model-affects-only-refresh
  (async done
    (let [calls (atom 0) deps {:measure-input (constantly 200)
                              :execute! (fn [_] (swap! calls inc)
                                          {:finish-reason :stop :conclusions [conclusion] :links []})}]
      (-> (core/refresh! deps input)
          (.then (fn [{:keys [run]}]
                   (is (= :current (core/freshness input run)))
                   (is (= :current (core/freshness (assoc-in input [:model :id] "model-2") run)))
                   (is (= :outdated (core/freshness (assoc-in input [:recipe :guidance] "Changed") run)))
                   (is (= :unavailable (core/freshness (assoc input :memories []) run)))
                   (-> (core/refresh! deps (assoc input :cached-run run))
                       (.then (fn [cached]
                                (is (= :reused (:status cached)))
                                (is (= 1 @calls))
                                (core/refresh! deps (-> input (assoc :cached-run run)
                                                       (assoc-in [:model :id] "model-2"))))))))
          (.then (fn [result]
                   (is (= :generated (:status result)))
                   (is (= 2 @calls)) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest invalid-or-truncated-output-does-not-produce-a-completed-run
  (async done
    (let [bad-items [(assoc conclusion :scope-type nil)
                     (assoc conclusion :unsupported "private")
                     (assoc conclusion :supporting-memory-ids ["invented"])
                     (assoc conclusion :basis :inferred)]]
      (-> (js/Promise.all
            (clj->js
              (concat
                (map (fn [item]
                       (-> (core/refresh! {:measure-input (constantly 200)
                                           :execute! (fn [_] {:finish-reason :stop :conclusions [item] :links []})} input)
                           (.catch config/error-code))) bad-items)
                [(-> (core/refresh! {:measure-input (constantly 200)
                                     :execute! (fn [_] {:finish-reason :length :conclusions [conclusion] :links []})} input)
                     (.catch config/error-code))])))
          (.then (fn [codes]
                   (is (= (repeat 5 :invalid-output) (array-seq codes))) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest comparison-is-minimized-and-only-inherited-as-access-dependency
  (async done
    (let [private-source (assoc (fixtures/observation "b" fixtures/other "user-2") :dropped? true)
          prior (assoc conclusion :id "old" :scope-subject-ids [fixtures/other]
                       :supporting-memory-ids ["b"])
          comparison {:id "old-run" :input-memory-ids ["b"] :conclusions [prior] :private "must-not-send"}
          request (assoc input :memories [(first (:memories input)) private-source]
                         :grants [{:owner-id fixtures/other :reader-id fixtures/own :permission "read"}]
                         :comparison comparison)]
      (-> (core/refresh! {:measure-input (constantly 200)
                          :execute! (fn [model-request]
                                      (is (not (.includes (:payload model-request) "must-not-send")))
                                      {:finish-reason :stop :conclusions [conclusion] :links []})} request)
          (.then (fn [{:keys [run]}]
                   (is (= ["a" "b"] (:input-memory-ids run)))
                   (is (= #{:comparison-dependency} (get (:dependency-roles run) "b")))
                   (is (= :unavailable (core/freshness (assoc request :grants []) run)))
                   (is (.startsWith (:id (first (:conclusions run))) "urn:adam:derived-observation:"))
                   (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest timeout-aborts-the-single-request
  (async done
    (let [calls (atom 0) signal (atom nil)]
      (-> (core/refresh! {:measure-input (constantly 200)
                          :execute! (fn [request]
                                      (swap! calls inc) (reset! signal (:signal request))
                                      (js/Promise. (fn [_ _])))}
                         (assoc-in input [:recipe :limits :timeout-ms] 5))
          (.then (fn [_] (is false "must time out") (done))
                 (fn [error]
                   (is (= :timeout (config/error-code error)))
                   (is (= 1 @calls)) (is (.-aborted @signal)) (done)))))))

(deftest empty-results-are-cached-and-input-order-does-not-change-fingerprint
  (async done
    (let [calls (atom 0) a (first (:memories input)) b (fixtures/observation "b" fixtures/own "user-2")
          deps {:measure-input (constantly 200)
                :execute! (fn [_] (swap! calls inc) {:finish-reason :stop :conclusions [] :links []})}
          request (assoc input :memories [a b])]
      (-> (core/refresh! deps request)
          (.then (fn [{:keys [run]}]
                   (is (empty? (:conclusions run)))
                   (core/refresh! deps (assoc request :cached-run run :memories [b a]))))
          (.then (fn [result] (is (= :reused (:status result))) (is (= 1 @calls)) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest independent-occasions-in-one-session-strengthen-inferred-conclusions
  (async done
    (let [request (update input :memories conj (fixtures/observation "b" fixtures/own "user-2"))
          item (assoc conclusion :basis :inferred :supporting-memory-ids ["a" "b"])]
      (-> (core/refresh! {:measure-input (constantly 200)
                          :execute! (fn [_] {:finish-reason :stop :conclusions [item] :links []})} request)
          (.then (fn [result] (is (= :generated (:status result))) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest named-or-all-refresh-is-sequential-and-isolates-failures
  (async done
    (let [started (atom []) active (atom 0) maximum (atom 0)
          recipes {"first" recipe "second" recipe}
          deps {:measure-input (constantly 200)
                :load-input! (fn [{:keys [name]}]
                               (swap! started conj name)
                               (if (= name "first")
                                 (js/Promise.reject (js/Error. "private-provider-data"))
                                 (js/Promise.resolve input)))
                :execute! (fn [_]
                            (swap! active inc) (swap! maximum max @active)
                            (-> (js/Promise.resolve {:finish-reason :stop :conclusions [] :links []})
                                (.finally #(swap! active dec))))}]
      (-> (core/refresh-configured! deps recipes {})
          (.then (fn [results]
                   (is (= ["first" "second"] @started))
                   (is (= [:failed :generated] (mapv :status results)))
                   (is (= :executor-failed (:code (first results))))
                   (is (not (.includes (pr-str results) "private-provider-data")))
                   (is (= 1 @maximum))
                   (reset! started [])
                   (core/refresh-configured! deps recipes {:name "unknown"})))
          (.then (fn [_] (is false "unknown configuration must reject") (done))
                 (fn [error]
                   (is (= :unknown-derivation (config/error-code error)))
                   (is (empty? @started)) (done)))))))

(deftest preflight-measures-the-complete-separate-request-and-reserves-model-capacity
  (let [captured (atom nil)
        prepared (core/prepare-refresh (assoc input :conversation "must-not-send-session-history")
                                       (fn [payload capabilities]
                                         (is (= "fake" (:provider capabilities)))
                                         (reset! captured payload) 200))
        payload (js/JSON.parse @captured)]
    (is (some? (aget payload "output-schema")))
    (is (= "Preserve scope." (aget (aget payload "configuration") "guidance")))
    (is (not (.includes @captured "must-not-send-session-history")))
    (is (= 8192 (get-in prepared [:request :max-output-tokens])))
    (is (= 10784 (aget (aget (js/JSON.parse (get-in prepared [:run :configuration-snapshot-json])) "effective-limits") "max-input-tokens"))))
  (doseq [measurement [-1 1.5 nil]]
    (try (core/prepare-refresh input (constantly measurement)) (is false "measurement must be valid")
         (catch :default error (is (= :invalid-budget (config/error-code error)))))))

(deftest changed-evidence-can-be-latest-and-outdated-without-an-extra-call
  (async done
    (let [calls (atom 0) current (atom input)]
      (-> (core/refresh! {:measure-input (constantly 200)
                          :execute! (fn [_]
                                      (swap! calls inc)
                                      (swap! current update :memories conj (fixtures/observation "b" fixtures/own "later"))
                                      {:finish-reason :stop :conclusions [conclusion] :links []})} input)
          (.then (fn [{:keys [run]}]
                   (is (= :outdated (core/freshness @current run)))
                   (is (= 1 @calls)) (is (= ["a"] (:input-memory-ids run))) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest source-content-is-limited-by-the-complete-request-not-a-sidecar-item-cap
  (let [request (-> input
                    (assoc-in [:model :context-window] 120000)
                    (assoc-in [:memories 0 :content] (.repeat "x" 70000)))]
    (is (= :prepared (:status (core/prepare-refresh request (constantly 12000)))))))

(deftest result-identities-separate-requesters-and-filter-buckets
  (is (not= (core/derivation-id fixtures/own "preferences" {:type :none})
            (core/derivation-id fixtures/own "preferences" {:type :language :value "python"})))
  (is (not= (core/derivation-id fixtures/own "preferences" {:type :none})
            (core/derivation-id fixtures/other "preferences" {:type :none}))))

(deftest output-links-can-contradict-within-a-run-and-supersede-compatible-history
  (async done
    (let [prior (assoc conclusion :id "old-conclusion")
          next-item (assoc conclusion :id "next" :content "A conflicting preference")
          request (assoc input :comparison {:id "prior" :input-memory-ids ["a"] :conclusions [prior]})
          links [{:relation :contradicts :from "c1" :to "next"}
                 {:relation :supersedes :from "c1" :to "old-conclusion"}]]
      (-> (core/refresh! {:measure-input (constantly 200)
                          :execute! (fn [_] {:finish-reason :stop :conclusions [conclusion next-item] :links links})} request)
          (.then (fn [{:keys [run]}]
                   (is (= "old-conclusion" (get-in run [:links 1 :to])))
                   (is (= (:id (first (:conclusions run))) (get-in run [:links 0 :from])))
                   (is (= (:id (second (:conclusions run))) (get-in run [:links 0 :to])))
                   (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))
