(ns adam.derivation.pi-controller-test
  (:require [adam.derivation.config :as config]
            [adam.derivation.pi :as pi]
            [adam.derivation.store :as store]
            [cljs.test :refer [async deftest is]]
            ["node:fs" :as fs]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(def recipes (config/parse-config "derivations:\n  a:\n    version: 1\n    guidance: Preserve scope.\n  b:\n    version: 1\n    guidance: Preserve uncertainty.\n"))
(def reader "urn:adam:user:test-reader")
(def model {:provider "fake" :id "model" :context-window 20000 :max-output-tokens 8192})
(def response {:finish-reason :stop :conclusions [] :links []})

(defn fixture [calls overrides]
  {:get-db! (fn [] (swap! calls conj :db) (js/Promise.resolve {:principal reader}))
   :provider! (fn [_]
                (swap! calls conj :provider)
                {:model model :measure-input (constantly 100)
                 :execute! (fn [_] (swap! calls conj :execute) (js/Promise.resolve response))})
   :operations (merge {:schema! (fn [_] (swap! calls conj :schema) (js/Promise.resolve nil))
                       :acquire! (fn [_ request _] (swap! calls conj [:acquire (:name request)]) (js/Promise.resolve {:status :acquired :token "token"}))
                       :renew! (fn [& _] (swap! calls conj :renew) (js/Promise.resolve {:status :renewed}))
                       :load! (fn [_ request] (swap! calls conj [:load (:name request)]) (js/Promise.resolve {:reader-id reader :grants [] :memories []}))
                       :publish! (fn [_ _ token run] (swap! calls conj [:publish token run]) (js/Promise.resolve {:status :published}))
                       :reuse! (fn [& _] (swap! calls conj :reuse) (js/Promise.resolve {:status :reused}))
                       :release! (fn [_ _ token code] (swap! calls conj [:release token code]) (js/Promise.resolve {:status :released}))}
                      overrides)})

(deftest schema-readiness-requires-an-online-native-entry-index-without-ddl
  (async done
    (let [attempt (fn [online?]
                    (let [queries (atom []) closed (atom false)
                          tx #js {:run (fn [query]
                                         (swap! queries conj query)
                                         (js/Promise.resolve #js {:records #js [#js {:get (fn [key]
                                                                                           (case key "labels" #js ["AdamDerivation" "AdamDerivationRun" "AdamDerivedObservation"]
                                                                                             "count" (if online? 1 0)))}]}))}
                          driver #js {:session (fn [_] #js {:executeRead (fn [work] (work tx))
                                                            :close (fn [] (reset! closed true) (js/Promise.resolve))})}
                          db (store/with-driver driver "neo4j" reader)]
                      (-> (store/check-schema! db)
                          (.then (fn [_] (is online?))
                                 (fn [error] (is (not online?)) (is (= :schema-unavailable (config/error-code error)))))
                          (.then (fn [_]
                                   (is @closed)
                                   (is (= 2 (count @queries)))
                                   (is (.includes (last @queries) "state = 'ONLINE'"))
                                   (is (not-any? #(.includes % "CREATE") @queries)))))))]
      (-> (js/Promise.all #js [(attempt false) (attempt true)])
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest configuration-reader-accumulates-short-reads-and-does-not-follow-symlinks
  (let [root (.mkdtempSync fs (join (tmpdir) "adam-derive-config-"))
        previous (aget js/process.env "XDG_CONFIG_HOME") original (aget fs "readSync")
        directory (join root "adam") filename (join directory "derivations.yaml")
        source "derivations:\n  a:\n    version: 1\n    guidance: First.\n  b:\n    version: 1\n    guidance: Last.\n"]
    (try
      (aset js/process.env "XDG_CONFIG_HOME" root)
      (.mkdirSync fs directory)
      (.writeFileSync fs filename source)
      (aset fs "readSync" (fn [fd buffer offset length position] (original fd buffer offset (min 3 length) position)))
      (is (= #{"a" "b"} (set (keys (pi/read-configuration)))))
      (aset fs "readSync" original)
      (.unlinkSync fs filename)
      (.writeFileSync fs (join root "outside.yaml") source)
      (.symlinkSync fs (join root "outside.yaml") filename)
      (try (pi/read-configuration) (is false "must not follow configuration symlink")
           (catch :default error (is (= :invalid-config (config/error-code error)))))
      (finally
        (aset fs "readSync" original)
        (if previous (aset js/process.env "XDG_CONFIG_HOME" previous) (js-delete js/process.env "XDG_CONFIG_HOME"))
        (.rmSync fs root #js {:recursive true :force true})))))

(deftest sequential-interface-preserves-success-and-isolates-failure
  (async done
    (let [calls (atom [])
          deps (fixture calls {:load! (fn [_ request]
                                      (swap! calls conj [:load (:name request)])
                                      (if (= "a" (:name request)) (js/Promise.reject (js/Error. "PRIVATE_BACKEND"))
                                          (js/Promise.resolve {:reader-id reader :grants [] :memories []})))})]
      (-> (pi/refresh-configured! deps recipes {})
          (.then (fn [outcomes]
                   (is (= [:failed :generated] (mapv :status outcomes)))
                   (is (= :executor-failed (:code (first outcomes))))
                   (is (not (.includes (pr-str outcomes) "PRIVATE")))
                   (is (= 1 (count (filter #{:execute} @calls))))
                   (is (some #{[:release "token" :executor-failed]} @calls))
                   (is (= [:provider :db :schema [:acquire "a"] [:load "a"] [:release "token" :executor-failed]
                           :provider :db :schema [:acquire "b"] [:load "b"] :execute]
                          (vec (take 12 @calls))))))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest busy-and-invalid-selector-never-load-or-execute
  (async done
    (let [calls (atom []) deps (fixture calls {:acquire! (fn [& _] (js/Promise.resolve {:status :busy}))})]
      (-> (pi/refresh-configured! deps recipes {:name "a"})
          (.then (fn [outcomes]
                   (is (= :busy (:status (first outcomes))))
                   (is (= [:provider :db :schema] @calls))
                   (reset! calls [])
                   (-> (pi/refresh-configured! deps recipes {:name "missing"})
                       (.then (fn [_] (is false "unknown name must fail"))
                              (fn [error] (is (= :unknown-derivation (config/error-code error))))))))
          (.then (fn [_] (is (empty? @calls))))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest empty-outcome-cache-reuse-releases-without-new-generation
  (async done
    (let [calls (atom []) saved (atom nil)
          deps (fixture calls {:publish! (fn [_ _ _ run] (reset! saved run) (js/Promise.resolve {:status :published}))
                               :load! (fn [& _] (swap! calls conj :load)
                                        (js/Promise.resolve {:reader-id reader :grants [] :memories [] :comparison @saved}))})]
      (-> (pi/refresh-configured! deps recipes {:name "a"})
          (.then (fn [_] (pi/refresh-configured! deps recipes {:name "a"})))
          (.then (fn [outcomes]
                   (is (= :reused (:status (first outcomes))))
                   (is (= 1 (count (filter #{:execute} @calls))))
                   (is (some #{:reuse} @calls))))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest failure-matrix-preserves-prior-results-and-releases-tokens
  (async done
    (let [attempt (fn [mode]
                    (let [calls (atom [])
                          recipe-set (cond-> recipes (= mode :timeout) (assoc-in ["a" :limits :timeout-ms] 5))
                          deps (-> (fixture calls (cond-> {}
                                                   (= mode :publish) (assoc :publish! (fn [& _] (js/Promise.reject (js/Error. "PRIVATE"))))
                                                   (= mode :fenced) (assoc :publish! (fn [& _] (js/Promise.resolve {:status :fenced})))) )
                                   (assoc :provider! (fn [_]
                                                      {:model model
                                                       :measure-input (constantly (if (= mode :budget) 999999 100))
                                                       :execute! (fn [_] (swap! calls conj :execute)
                                                                   (case mode
                                                                     :timeout (js/Promise. (fn [& _]))
                                                                     :output {:finish-reason :length :conclusions [] :links []}
                                                                     (js/Promise.resolve response)))})))]
                      (-> (pi/refresh-configured! deps recipe-set {:name "a"})
                          (.then (fn [outcomes]
                                   (let [outcome (first outcomes)]
                                     (is (= :failed (:status outcome)))
                                     (is (= (get {:timeout :timeout :budget :input-too-large :output :invalid-output
                                                  :publish :executor-failed :fenced :cancelled} mode) (:code outcome)))
                                     (is (some #{[:release "token" (:code outcome)]} @calls))
                                     (when (= mode :budget)
                                       (is (not-any? #{:execute} @calls))
                                       (is (= 999999 (:measured-input-tokens outcome)))
                                       (is (pos? (:available-input-tokens outcome))))))))))]
      (-> (js/Promise.all (to-array (map attempt [:timeout :budget :output :publish :fenced])))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest explicit-command-validation-and-shutdown-do-not-generate-automatically
  (async done
    (let [commands (atom {}) calls (atom []) ui (atom [])
          extension #js {:registerCommand (fn [name command] (swap! commands assoc name command))}
          controller-fixture (fixture calls {})
          runtime (pi/register-command! extension
                    {:configured? true :get-db! (fn [_] ((:get-db! controller-fixture)))
                     :load-config! (constantly recipes) :operations (:operations controller-fixture)
                     :provider-fn (fn [_ recipe] ((:provider! controller-fixture) recipe))})
          ctx #js {:waitForIdle (fn [] (swap! calls conj :idle) (js/Promise.resolve))
                    :ui #js {:notify (fn [text _] (swap! ui conj text))}}
          invoke (fn [args] (.call (aget (get @commands "adam:derive") "handler") nil args ctx))]
      (is (empty? @calls) "registration is inert")
      (is (= {:name "a" :language "py"} (pi/parse-command "a --language py")))
      (-> (invoke "unknown")
          (.then (fn [_] (is (empty? @calls)) (is (.includes (last @ui) "unknown-derivation")) (invoke "a")))
          (.then (fn [result]
                   (is (undefined? result))
                   (is (= :idle (first @calls)))
                   (is (some #{:execute} @calls))
                   (is (= "a: generated" (last @ui)))
                   ((:shutdown! runtime))))
          (.then (fn [_] (reset! calls []) (invoke "a")))
          (.then (fn [_] (is (empty? @calls)) (is (.includes (last @ui) "cancelled"))))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest shutdown-aborts-inflight-model-work-and-releases-before-closing
  (async done
    (let [commands (atom {}) calls (atom []) started (js/Promise.withResolvers) captured (atom nil)
          fixture (fixture calls {})
          runtime (pi/register-command! #js {:registerCommand (fn [name command] (swap! commands assoc name command))}
                    {:configured? true :get-db! (fn [_] ((:get-db! fixture))) :load-config! (constantly recipes)
                     :operations (:operations fixture)
                     :provider-fn (fn [_ _] {:model model :measure-input (constantly 100)
                                              :execute! (fn [request]
                                                          (reset! captured (:signal request)) ((.-resolve started))
                                                          (js/Promise. (fn [& _])))})})
          ctx #js {:waitForIdle (fn [] (js/Promise.resolve)) :ui #js {:notify (fn [& _])}}
          command (.call (aget (get @commands "adam:derive") "handler") nil "a" ctx)]
      (-> (.-promise started)
          (.then (fn [_] ((:shutdown! runtime))))
          (.then (fn [_] command))
          (.then (fn [_]
                   (is (.-aborted @captured))
                   (is (some #{[:release "token" :cancelled]} @calls))
                   (is (not-any? #(and (vector? %) (= :publish (first %))) @calls))))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest renewal-loss-cancels-and-fences-generation
  (async done
    (let [calls (atom []) captured (atom nil)
          deps (-> (fixture calls {:renew! (fn [& _] (swap! calls conj :renew) (js/Promise.resolve {:status :fenced}))})
                   (assoc :lease-ms 30 :renewal-ms 5
                          :provider! (fn [_] {:model model :measure-input (constantly 100)
                                              :execute! (fn [request] (reset! captured (:signal request)) (js/Promise. (fn [& _])))})))]
      (-> (pi/refresh-configured! deps recipes {:name "a"})
          (.then (fn [outcomes]
                   (is (= :cancelled (:code (first outcomes))))
                   (is (.-aborted @captured))
                   (is (some #{[:release "token" :cancelled]} @calls))
                   (is (not-any? #(and (vector? %) (= :publish (first %))) @calls))))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))
