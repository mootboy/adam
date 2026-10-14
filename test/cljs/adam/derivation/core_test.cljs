(ns adam.derivation.core-test
  (:require [adam.derivation.config :as config]
            [adam.derivation.core :as core]
            [cljs.test :refer [async deftest is testing]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop :include-macros true]))

(def own "urn:adam:user:owner")
(def other "urn:adam:user:other")
(def session "urn:adam:session:owner:pi:session-a")
(defn observation [id owner entry]
  {:id id :owner-id owner :session-id session :kind :observation
   :producer "test-producer" :content "A source-grounded preference"
   :source-occasions [{:session-id session :stream-id "main" :entry-id entry}]})

(def recipe (get (config/parse-config "derivations:\n  preferences:\n    version: 1\n    guidance: Preserve scope.\n") "preferences"))
(def model {:provider "fake" :id "model-1" :context-window 20000 :max-output-tokens 8192})
(def input {:reader-id own :grants [] :name "preferences" :filter {:type :none}
            :recipe recipe :model model :memories [(observation "a" own "user-1")]})
(def conclusion {:id "c1" :content "Prefer simple boundaries" :category "architecture"
                 :basis :explicit :scope-type :personal :scope-subject-ids [own]
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
    (let [private-source (assoc (observation "b" other "user-2") :dropped? true)
          prior (assoc conclusion :id "old" :scope-subject-ids [other]
                       :supporting-memory-ids ["b"])
          comparison {:id "old-run" :input-memory-ids ["b"] :conclusions [prior] :private "must-not-send"}
          request (assoc input :memories [(first (:memories input)) private-source]
                         :grants [{:owner-id other :reader-id own :permission "read"}]
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
    (let [calls (atom 0) a (first (:memories input)) b (observation "b" own "user-2")
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
    (let [request (update input :memories conj (observation "b" own "user-2"))
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
                                      (swap! current update :memories conj (observation "b" own "later"))
                                      {:finish-reason :stop :conclusions [conclusion] :links []})} input)
          (.then (fn [{:keys [run]}]
                   (is (= :outdated (core/freshness @current run)))
                   (is (= 1 @calls)) (is (= ["a"] (:input-memory-ids run))) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest source-content-is-limited-by-the-complete-request-not-a-sidecar-item-cap
  (async done
    (let [request (-> input
                      (assoc-in [:model :context-window] 120000)
                      (assoc-in [:memories 0 :content] (.repeat "x" 70000)))]
      (-> (core/refresh! {:measure-input (constantly 12000)
                          :execute! (fn [_] {:finish-reason :stop :conclusions [conclusion] :links []})} request)
          (.then (fn [result] (is (= :generated (:status result))) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest result-identities-separate-requesters-and-filter-buckets
  (async done
    (let [deps {:measure-input (constantly 200)
                :execute! (fn [_] {:finish-reason :stop :conclusions [] :links []})}
          requests [input (assoc input :reader-id other) (assoc input :filter {:type :language :value "python"})]]
      (-> (js/Promise.all (to-array (map #(core/refresh! deps %) requests)))
          (.then (fn [results]
                   (let [ids (map #(get-in % [:run :derivation-id]) (array-seq results))]
                     (is (= 3 (count (set ids))))) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

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

;; Test the refresh boundary, not the internal selection/role reducers.
(defn attempt! [request response]
  (let [calls (atom [])]
    (-> (core/refresh! {:measure-input (constantly 200)
                       :new-run-id (constantly "test-run")
                       :now (constantly "2026-10-12T00:00:00Z")
                       :execute! (fn [model-request] (swap! calls conj model-request) response)}
                      request)
        (.then (fn [result] {:result result :calls @calls})
               (fn [error] {:code (config/error-code error) :calls @calls})))))

(deftest evidence-contracts-through-the-refresh-interface
  (async done
    (let [a (observation "a" own "one")
          b (observation "b" own "two")
          r {:id "r" :owner-id own :session-id session :kind :reflection
             :producer "test-producer" :content "A synthesized preference" :supporting-memory-ids ["a" "b"]}
          inferred (assoc conclusion :basis :inferred :supporting-memory-ids ["a" "b"])
          cases [{:label "genuine repetition in one session" :memories [a b] :claim inferred :generated? true}
                 {:label "duplicate summaries are not repetition" :memories [a (observation "b" own "one")] :claim inferred}
                 {:label "reflection repeats its supports, not another occasion" :memories [a (observation "b" own "one") r]
                  :claim (assoc inferred :supporting-memory-ids ["a" "r"])}
                 {:label "dropped support still grounds a reflection" :memories [(assoc a :dropped? true) (assoc b :dropped? true) r]
                  :claim (assoc inferred :supporting-memory-ids ["r"]) :generated? true :candidates ["r"]}
                 {:label "original anchors survive fork recording" :memories [(assoc a :session-id (str session "-fork"))
                                                                             (assoc b :session-id (str session "-fork"))]
                  :claim inferred :generated? true}
                 {:label "copied ownership namespace does not manufacture repetition"
                  :memories [a (assoc (observation "b" other "one") :session-id "urn:adam:session:other:pi:session-a"
                                    :source-occasions [{:session-id "urn:adam:session:other:pi:session-a" :stream-id "main" :entry-id "one"}])]
                  :grants [{:owner-id other :reader-id own :permission "read"}] :claim inferred}]]
      (-> (js/Promise.all
            (to-array (map (fn [{:keys [memories claim grants] :as case}]
                             (-> (attempt! (assoc input :memories memories :grants grants)
                                           {:finish-reason :stop :conclusions [claim] :links []})
                                 (.then #(vector case %)))) cases)))
          (.then (fn [pairs]
                   (doseq [[{:keys [label generated? candidates]} {:keys [result code calls]}] (array-seq pairs)]
                     (testing label
                       (is (= (if generated? :generated nil) (:status result)))
                       (is (= (when-not generated? :invalid-output) code))
                       (is (= 1 (count calls)))
                       (when candidates
                         (is (= candidates (js->clj (aget (js/JSON.parse (:payload (first calls))) "candidate-memory-ids")))))))
                   (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest dependency-and-sharing-contracts-through-refresh
  (async done
    (let [a (observation "a" own "one")
          shared (assoc (observation "b" other "two") :dropped? true)
          prior {:id "prior" :input-memory-ids ["b"]
                 :conclusions [(assoc conclusion :id "old" :content "PRIVATE"
                                      :scope-subject-ids [other] :supporting-memory-ids ["b"])]}
          cases [{:label "direct read grant" :memories [a (observation "b" other "two")]
                  :grants [{:owner-id other :reader-id own :permission "read"}] :ids ["a" "b"]}
                 {:label "reverse grant is not access" :memories [a (observation "b" other "two")]
                  :grants [{:owner-id own :reader-id other :permission "read"}] :ids ["a"]}
                 {:label "grants do not transitively propagate" :memories [a (observation "b" other "two") (observation "c" "third" "three")]
                  :grants [{:owner-id other :reader-id own :permission "read"} {:owner-id "third" :reader-id other :permission "read"}]
                  :ids ["a" "b"]}
                 {:label "comparison inherits authorized dependencies" :memories [a shared] :comparison prior
                  :grants [{:owner-id other :reader-id own :permission "read"}] :ids ["a" "b"] :comparison? true}
                 {:label "revocation withholds comparison" :memories [a shared] :comparison prior :ids ["a"]
                  :diagnostics [:comparison-unavailable]}
                 {:label "empty dependency claim cannot carry private text" :memories [a] :comparison (assoc prior :input-memory-ids [])
                  :ids ["a"] :diagnostics [:comparison-unavailable]}
                 {:label "missing reflection support fails before execution" :memories [{:id "r" :owner-id own :session-id session
                                                                                       :kind :reflection :producer "test"
                                                                                       :content "Preference" :supporting-memory-ids ["missing"]}]
                  :code :unresolved-evidence}
                 {:label "private reflection support fails closed" :memories [(observation "b" other "two")
                                                                             {:id "r" :owner-id own :session-id session
                                                                              :kind :reflection :producer "test"
                                                                              :content "Preference" :supporting-memory-ids ["b"]}]
                  :code :unauthorized}
                 {:label "duplicate source identities fail before execution" :memories [a a] :code :invalid-memory}]]
      (-> (js/Promise.all
            (to-array (map (fn [case]
                             (-> (attempt! (merge input (select-keys case [:memories :grants :comparison]))
                                           {:finish-reason :stop :conclusions [] :links []})
                                 (.then #(vector case %)))) cases)))
          (.then (fn [pairs]
                   (doseq [[{:keys [label ids diagnostics comparison? code] :as case} result] (array-seq pairs)]
                     (testing label
                       (is (= code (:code result)))
                       (if code
                         (is (empty? (:calls result)))
                         (let [run (get-in result [:result :run])
                               payload (js/JSON.parse (:payload (first (:calls result))))]
                           (is (= ids (:input-memory-ids run)))
                           (is (= (or diagnostics []) (:diagnostics run)))
                           (is (= (boolean comparison?) (some? (aget payload "comparison"))))))))
                   (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest selectors-through-configured-refresh-and-read-interfaces
  (async done
    (let [a (assoc (observation "a" own "one") :linked-paths ["src/A.PY"] :repository-origins ["github.com/example/repo"])
          b (observation "b" own "two")
          run (fn [selector]
                (core/refresh-configured!
                  {:load-input! (fn [_] (assoc input :memories [a b]))
                   :measure-input (constantly 200)
                   :execute! (fn [_] {:finish-reason :stop :conclusions [] :links []})}
                  {"preferences" recipe} selector))
          cases [[{} ["a" "b"]]
                 [{:language "PY"} ["a"]]
                 [{:repository "git@github.com:example/repo.git"} ["a"]]
                 [{:repository "https://user:secret@github.com/example/repo.git"} ["a"]]
                 [{:session session} ["a" "b"]]]]
      (-> (js/Promise.all
            (to-array (map (fn [[selector ids]] (-> (run selector) (.then #(vector selector ids %)))) cases)))
          (.then (fn [rows]
                   (doseq [[selector ids results] (array-seq rows)]
                     (testing (pr-str selector)
                       (is (= ids (get-in (first results) [:run :input-memory-ids])))))
                   (let [read (core/normalize-read {})]
                     (is (= {:filter nil :history? false :limit 10} read)))
                   (doseq [request [{:history "true"} {:limit 21} {:limit 0} {:user "other"}]]
                     (try (core/normalize-read request) (is false "invalid read request")
                          (catch :default error (is (= :invalid-selector (config/error-code error))))))
                   (js/Promise.all
                     (to-array (map (fn [request] (-> (run request) (.catch config/error-code)))
                                    [{:repository "git@github.com:example/repo" :language "python"}
                                     {:session "session-a"} {:language "unknown"} {:name nil}
                                     {:reader-id "impersonation"} {:repository nil}])))))
          (.then (fn [codes] (is (= (repeat 6 :invalid-selector) (array-seq codes))) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest generated-read-selectors-preserve-origin-equivalence
  (let [result (tc/quick-check
                 80
                 (prop/for-all [suffix (gen/choose 1 100000) limit (gen/choose 1 20) history gen/boolean]
                   (let [repo (str "example/repo-" suffix)
                         request {:limit limit :history history}]
                     (= (core/normalize-read (assoc request :repository (str "git@github.com:" repo ".git")))
                        (core/normalize-read (assoc request :repository (str "https://user:secret@github.com/" repo ".git"))))))
                 :seed 20261012)]
    (is (:pass? result) (pr-str result))))

(deftest generated-evidence-permutations-have-identical-snapshots
  (let [result (tc/quick-check
                 80
                 (prop/for-all [ids (gen/vector-distinct (gen/choose 0 30) {:min-elements 1 :max-elements 10})]
                   (let [memories (mapv #(observation (str "m-" %) own (str "user-" %)) ids)
                         prepare #(core/prepare-refresh (assoc input :memories %) (constantly 200))]
                     (= (:run (prepare memories)) (:run (prepare (vec (reverse memories)))))))
                 :seed 20261012)]
    (is (:pass? result) (pr-str result))))

(deftest runtime-effects-do-not-reread-source-state-during-processing
  (async done
    (let [events (atom [])]
      (-> (core/refresh-configured!
            {:load-input! (fn [_] (swap! events conj :read) input)
             :measure-input (constantly 200)
             :new-run-id (fn [] (swap! events conj :identity) "captured-run")
             :now (fn [] (swap! events conj :clock) "captured-time")
             :execute! (fn [_] (swap! events conj :execute)
                         {:finish-reason :stop :conclusions [conclusion] :links []})}
            {"preferences" recipe} {})
          (.then (fn [results]
                   (is (= [:read :identity :clock :execute] @events))
                   (is (= "captured-time" (get-in (first results) [:run :created-at]))) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest comparison-citations-cannot-omit-authorization-dependencies
  (async done
    (let [bob (assoc (observation "b" other "two") :dropped? true)
          cases [[["b"] []] [["a"] ["b"]]]
          call (fn [[supports conflicts]]
                 (let [prior (assoc conclusion :id "prior-claim" :content "PRIVATE_BOB"
                                    :supporting-memory-ids supports :conflicting-memory-ids conflicts)
                       request (assoc input :memories [(first (:memories input)) bob]
                                      :comparison {:id "prior" :input-memory-ids ["a"] :conclusions [prior]})]
                   (attempt! request {:finish-reason :stop :conclusions [] :links []})))]
      (-> (js/Promise.all (to-array (map call cases)))
          (.then (fn [results]
                   (doseq [{:keys [result calls]} (array-seq results)]
                     (is (= :generated (:status result)))
                     (is (= ["a"] (get-in result [:run :input-memory-ids])))
                     (is (= [:comparison-unavailable] (get-in result [:run :diagnostics])))
                     (is (not (.includes (:payload (first calls)) "PRIVATE_BOB"))))
                   (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest malformed-and-graph-loaded-comparison-conclusions
  (async done
    (let [prior (assoc conclusion :id "prior-claim" :basis "explicit" :scope-type "personal")
          call (fn [relation]
                 (attempt! (assoc input :comparison {:id "prior" :input-memory-ids ["a"] :conclusions [prior]})
                           {:finish-reason :stop :conclusions [conclusion]
                            :links [{:relation relation :from "c1" :to "prior-claim"}]}))]
      (-> (js/Promise.all (to-array (map call [:contradicts :supersedes])))
          (.then (fn [results]
                   (doseq [{:keys [result code]} (array-seq results)]
                     (is (nil? code))
                     (is (= :generated (:status result)))
                     (is (= "prior-claim" (get-in result [:run :links 0 :to]))))
                   (attempt! (assoc input :comparison {:id "broken" :input-memory-ids ["a"]
                                                       :conclusions [(dissoc conclusion :supporting-memory-ids)]})
                             {:finish-reason :stop :conclusions [] :links []})))
          (.then (fn [{:keys [result]}]
                   (is (= [:comparison-unavailable] (get-in result [:run :diagnostics]))) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest source-text-preserves-terminal-control-characters
  (async done
    (let [request (assoc-in input [:memories 0 :content] "Source finding \u001b[31mred\u001b[0m")]
      (-> (attempt! request {:finish-reason :stop :conclusions [conclusion] :links []})
          (.then (fn [{:keys [result calls code]}]
                   (is (nil? code))
                   (is (= :generated (:status result)))
                   (is (= (get-in request [:memories 0 :content])
                          (aget (aget (aget (js/JSON.parse (:payload (first calls))) "memories") 0) "content")))
                   (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest raw-memory-remotes-match-without-forwarding-credentials
  (async done
    (let [call (fn [origin]
                 (core/refresh-configured!
                   {:load-input! (fn [_] (assoc-in input [:memories 0 :repository-origins] [origin]))
                    :measure-input (constantly 200)
                    :execute! (fn [request]
                                (is (not (.includes (:payload request) "secret")))
                                {:finish-reason :stop :conclusions [] :links []})}
                   {"preferences" recipe} {:repository "git@github.com:example/repo.git"}))]
      (-> (js/Promise.all
            (to-array (map call ["git@github.com:example/repo.git" "https://user:secret@github.com/example/repo.git"
                                "github.com/example/repo"])))
          (.then (fn [results]
                   (doseq [rows (array-seq results)]
                     (is (= ["a"] (get-in (first rows) [:run :input-memory-ids]))))
                   (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest configured-refresh-keeps-actionable-budget-diagnostics
  (async done
    (-> (core/refresh-configured!
          {:load-input! (fn [_] input) :measure-input (constantly 30000)
           :execute! (fn [_] (is false "oversized input must not call executor"))}
          {"preferences" recipe} {})
        (.then (fn [[result]]
                 (is (= :input-too-large (:code result)))
                 (is (= 30000 (:measured-input-tokens result)))
                 (is (= 10784 (:available-input-tokens result)))
                 (done)))
        (.catch (fn [error] (is false (.-stack error)) (done))))))

(deftest wrong-bucket-cache-is-not-reused-and-invalid-readers-are-unavailable
  (async done
    (let [deps {:measure-input (constantly 200)
                :execute! (fn [_] {:finish-reason :stop :conclusions [] :links []})}]
      (-> (core/refresh! deps input)
          (.then (fn [{:keys [run]}]
                   (let [reader ""
                         malformed (assoc input :reader-id reader
                                          :grants [{:owner-id own :reader-id reader :permission "read"}])
                         forged-bucket (assoc run :derivation-id (core/derivation-id reader "preferences" {:type :none}))]
                     (is (= :unavailable (core/freshness malformed forged-bucket))))
                   (core/refresh! deps (assoc input :name "different" :cached-run run))))
          (.then (fn [result]
                   (is (= :generated (:status result)))
                   (is (= (core/derivation-id own "different" {:type :none}) (get-in result [:run :derivation-id])))
                   (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest language-filter-requires-an-extension-in-the-file-basename
  (async done
    (let [cases [["go" "go"] ["c" "c"] ["rs" "rust"] ["foo.py/go" "python"] ["foo/go" "go"]]
          call (fn [[path language]]
                 (core/refresh-configured!
                   {:load-input! (fn [_] (assoc-in input [:memories 0 :linked-paths] [path]))
                    :measure-input (constantly 200)
                    :execute! (fn [_] {:finish-reason :stop :conclusions [] :links []})}
                   {"preferences" recipe} {:language language}))]
      (-> (js/Promise.all (to-array (map call cases)))
          (.then (fn [results]
                   (doseq [rows (array-seq results)]
                     (is (= [] (get-in (first rows) [:run :input-memory-ids]))))
                   (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest malformed-cached-conclusions-do-not-relax-dependency-access
  (async done
    (let [deps {:measure-input (constantly 200)
                :execute! (fn [_] {:finish-reason :stop :conclusions [conclusion] :links []})}]
      (-> (core/refresh! deps input)
          (.then (fn [{:keys [run]}]
                   (let [forged (assoc-in run [:conclusions 0 :conflicting-memory-ids] ["b"])]
                     (is (= :unavailable (core/freshness input forged)))
                     (core/refresh! deps (assoc input :cached-run forged)))))
          (.then (fn [result] (is (= :generated (:status result))) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest all-config-refresh-captures-one-snapshot-per-name-before-its-model-call
  (async done
    (let [events (atom [])
          deps {:load-input! (fn [{:keys [name]}] (swap! events conj [:read name])
                               (assoc-in input [:memories 0 :content] name))
                :measure-input (constantly 200)
                :execute! (fn [{:keys [payload]}]
                            (let [name (aget (aget (aget (js/JSON.parse payload) "memories") 0) "content")]
                              (swap! events conj [:execute name])
                              (js/Promise.resolve {:finish-reason :stop :conclusions [] :links []})))}]
      (-> (core/refresh-configured! deps {"first" recipe "second" recipe} {})
          (.then (fn [results]
                   (is (= [:generated :generated] (mapv :status results)))
                   (is (= [[:read "first"] [:execute "first"] [:read "second"] [:execute "second"]] @events))
                   (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest repository-selectors-reject-ambiguous-shorthand-before-loading
  (async done
    (let [deps {:load-input! (fn [_] (is false "invalid selectors must not load evidence"))
                :execute! (fn [_] (is false "invalid selectors must not call the model"))}]
      (-> (js/Promise.all
            (to-array (map #(-> (core/refresh-configured! deps {"preferences" recipe} {:repository %})
                               (.catch config/error-code))
                           ["foo/bar" "github.com/example/repo"])))
          (.then (fn [codes]
                   (is (= [:invalid-selector :invalid-selector] (vec (array-seq codes))))
                   (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest malformed-comparison-only-memories-are-withheld-without-blocking-source
  (async done
    (let [historical (assoc (observation "b" other "two") :dropped? true)
          invalid [(assoc historical :content nil) (dissoc historical :producer)
                   (assoc historical :session-id nil)]
          call (fn [memory]
                 (attempt! (assoc input :memories [(first (:memories input)) memory]
                                  :grants [{:owner-id other :reader-id own :permission "read"}]
                                  :comparison {:id "prior" :input-memory-ids ["b"]
                                               :conclusions [(assoc conclusion :id "old" :content "PRIVATE_BOB"
                                                                    :supporting-memory-ids ["b"])]})
                           {:finish-reason :stop :conclusions [conclusion] :links []}))]
      (-> (js/Promise.all (to-array (map call invalid)))
          (.then (fn [results]
                   (doseq [{:keys [result code calls]} (array-seq results)]
                     (is (nil? code))
                     (is (= :generated (:status result)))
                     (is (= ["a"] (get-in result [:run :input-memory-ids])))
                     (is (= [:comparison-unavailable] (get-in result [:run :diagnostics])))
                     (is (not (.includes (:payload (first calls)) "PRIVATE_BOB"))))
                   (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest model-visible-rules-and-content-free-output-rejections
  (async done
    (let [a (observation "a" own "one") b (observation "b" other "two")
          h (assoc (observation "h" own "one") :dropped? true)
          r {:id "r" :owner-id own :session-id session :kind :reflection :producer "test"
             :content "Repeated historical finding" :supporting-memory-ids ["h"]}
          configured (assoc recipe :categories {"architecture" "Architecture"})
          source (assoc input :recipe configured :memories [a b h r]
                              :grants [{:owner-id other :reader-id own :permission "read"}])
          bob (assoc conclusion :id "bob" :scope-subject-ids [other] :supporting-memory-ids ["b"] :conflicting-memory-ids ["a"])
          alice (assoc conclusion :conflicting-memory-ids ["b"])
          prior {:id "prior" :input-memory-ids ["a"] :conclusions [(assoc conclusion :id "old")]}
          cases [{:label "attributed disagreement without cross-subject link" :conclusions [alice bob]}
                 {:label "cross-subject link" :conclusions [alice bob]
                  :links [{:relation :contradicts :from "c1" :to "bob"}] :reason :link-scope-mismatch}
                 {:label "same-scope contradiction" :conclusions [conclusion (assoc conclusion :id "second")]
                  :links [{:relation :contradicts :from "c1" :to "second"}]}
                 {:label "new-run supersession is invalid" :conclusions [conclusion (assoc conclusion :id "second")]
                  :links [{:relation :supersedes :from "c1" :to "second"}] :reason :supersedes-target-not-prior}
                 {:label "authorized prior scoped supersession" :comparison prior :conclusions [conclusion]
                  :links [{:relation :supersedes :from "c1" :to "old"}]}
                 {:label "invented subject" :conclusions [(assoc conclusion :scope-subject-ids ["Alice"])] :reason :subject-not-in-evidence}
                 {:label "historical support cannot support conclusion" :conclusions [(assoc conclusion :supporting-memory-ids ["h"])] :reason :support-not-eligible}
                 {:label "invented conflicting evidence" :conclusions [(assoc conclusion :conflicting-memory-ids ["missing"])] :reason :conflict-not-eligible}
                 {:label "one occasion cannot support inference" :conclusions [(assoc conclusion :basis :inferred)] :reason :inferred-below-threshold}
                 {:label "summary copy does not add occasion" :conclusions [(assoc conclusion :basis :inferred :supporting-memory-ids ["a" "r"])] :reason :inferred-below-threshold}
                 {:label "two distinct occasions support inference" :conclusions [(assoc conclusion :basis :inferred :supporting-memory-ids ["a" "b"])]}
                 {:label "unconfigured category" :conclusions [(assoc conclusion :category "style")] :reason :category-not-configured}
                 {:label "uppercase category is malformed" :conclusions [(assoc conclusion :category "Architecture")] :reason :response-shape}
                 {:label "scope disabled by recipe" :recipe (assoc configured :scope-types #{"repository"}) :conclusions [conclusion] :reason :scope-not-configured}
                 {:label "prior ID cannot be reused" :comparison prior :conclusions [(assoc conclusion :id "old")] :reason :conclusion-id-collision}
                 {:label "invented endpoint" :conclusions [conclusion]
                  :links [{:relation :contradicts :from "c1" :to "missing"}] :reason :link-endpoint}]
          attempt (fn [{:keys [reason recipe comparison conclusions links label]}]
                    (let [request (cond-> (assoc source :recipe (or recipe configured)) comparison (assoc :comparison comparison))]
                      (-> (core/refresh-configured!
                           {:load-input! (fn [_] request) :measure-input (constantly 200)
                            :execute! (fn [{:keys [payload]}]
                                        (let [raw (js/JSON.parse payload) rules (aget raw "output-rules")]
                                          (is (some? rules))
                                          (is (= #{own other} (set (array-seq (aget (aget rules "subject-ids") "personal")))))
                                          (is (not (some #{"h"} (array-seq (aget rules "supporting-memory-ids")))))
                                          (is (some #{"h"} (array-seq (aget rules "conflicting-memory-ids"))))
                                          (is (= (js/JSON.stringify (aget (aget rules "verified-occasion-keys") "a"))
                                                 (js/JSON.stringify (aget (aget rules "verified-occasion-keys") "r"))))
                                          (is (.includes (aget raw "instructions") "not a cross-subject link"))
                                          {:finish-reason :stop :conclusions conclusions :links (or links [])}))}
                           {"preferences" (:recipe request)} {:name "preferences"})
                          (.then (fn [[outcome]]
                                   (testing label
                                     (is (= reason (:validation-reason outcome)))
                                     (is (= (if reason :failed :generated) (:status outcome)))
                                     (when reason
                                       (is (= :invalid-output (:code outcome)))
                                       (is (not (.includes (js/JSON.stringify (clj->js outcome)) "Source-grounded preference"))))))))))]
      (-> (js/Promise.all (to-array (map attempt cases)))
          (.then (fn [_] (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest configured-refresh-forwards-exact-raw-selectors-for-named-and-all-recipes
  (async done
    (let [origin "git@github.com:x/y.git"
          cases [{:request {:name "preferences" :repository origin}
                  :recipes {"preferences" recipe} :expected [{:name "preferences" :repository origin}]}
                 {:request {:repository origin} :recipes {"first" recipe "second" recipe}
                  :expected [{:name "first" :repository origin} {:name "second" :repository origin}]}]
          attempt (fn [{:keys [request recipes expected]}]
                    (let [captured (atom [])
                          deps {:load-input! (fn [request] (swap! captured conj request) input)
                                :measure-input (constantly 100)
                                :execute! (constantly {:finish-reason :stop :conclusions [] :links []})}]
                      (-> (core/refresh-configured! deps recipes request)
                          (.then (fn [outcomes]
                                   (is (every? #(= :generated (:status %)) outcomes))
                                   (is (= expected @captured)))))))]
      (-> (js/Promise.all (to-array (map attempt cases)))
          (.then (fn [_] (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))
