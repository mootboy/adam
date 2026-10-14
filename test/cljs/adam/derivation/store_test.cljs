(ns adam.derivation.store-test
  (:require [adam.derivation.config :as config]
            [adam.derivation.core :as core]
            [adam.derivation.store :as store]
            [cljs.test :refer [async deftest is testing]]))

(def reader "urn:adam:user:test-reader")

(defn recording-driver
  ([reply] (recording-driver reply ["AdamDerivation" "AdamDerivationRun" "AdamDerivedObservation"]))
  ([reply schema-labels]
  (let [calls (atom []) closes (atom 0) transactions (atom 0)
        run (fn [query params]
              (let [params (js->clj params :keywordize-keys true)]
                (swap! calls conj {:query query :params params})
                (js/Promise.resolve
                  #js {:records (to-array
                                 (map (fn [row] #js {:get (fn [key] (get row key))})
                                      (cond
                                        (re-find #"RETURN u.id AS userId" query) [{"userId" reader}]
                                        (re-find #"SHOW CONSTRAINTS" query) [{"labels" (if (fn? schema-labels) (schema-labels query) schema-labels)}]
                                        :else (reply query params))))})))
        tx #js {:run run}
        session #js {:run run :executeWrite (fn [f] (swap! transactions inc) (f tx))
                     :executeRead (fn [f] (swap! transactions inc) (f tx))
                     :close (fn [] (swap! closes inc) (js/Promise.resolve nil))}]
    {:driver #js {:session (fn [_] session)} :calls calls :closes closes :transactions transactions})))

(deftest saved-read-snapshot-is-empty-without-source-streaming-or-ddl
  (async done
    (let [{:keys [driver calls closes transactions]} (recording-driver (fn [_ _] []))
          db (store/with-driver driver "neo4j" reader)]
      (-> (store/read-snapshot! db {:history true :limit 2})
          (.then (fn [snapshot]
                   (is (= {:reader-id reader :memories [] :grants [] :items [] :truncated? false} snapshot))
                   (is (= 1 @transactions))
                   (is (= 1 @closes))
                   (is (= 1 (count @calls)))
                   (is (not-any? #(re-find #"CREATE|SET|DELETE|rawJson" (:query %)) @calls))))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest aborting-a-saved-read-closes-its-session-and-pre-abort-opens-nothing
  (async done
    (let [opened (atom 0) closed (atom 0) entered (js/Promise.withResolvers)
          work (js/Promise.withResolvers) winding-down (js/Promise.withResolvers) settled? (atom false)
          driver #js {:session (fn [_]
                                 (swap! opened inc)
                                 #js {:executeRead (fn [_] ((.-resolve entered)) (.-promise work))
                                      :close (fn [] (swap! closed inc) ((.-reject work) (js/Error. "cancelled backend")) (.-promise winding-down))})}
          db (store/with-driver driver "neo4j" reader) control (js/AbortController.)
          result (-> (store/read-snapshot! db {} (.-signal control)) (.finally #(reset! settled? true)))]
      (-> (.-promise entered)
          (.then (fn [_] (.abort control) (js/Promise.resolve)))
          (.then (fn [_]
                   (is (false? @settled?) "read does not finish before first session.close winds down")
                   ((.-resolve winding-down))
                   (-> result (.catch config/error-code))))
          (.then (fn [code]
                   (is (= :cancelled code)) (is (pos? @closed))
                   (store/read-snapshot! db {} (.-signal control))))
          (.catch (fn [error] (is (= :cancelled (config/error-code error))) (is (= 1 @opened))))
          (.finally done)))))

(deftest lease-interface-keeps-principal-and-bucket-fixed
  (async done
    (let [{:keys [driver calls closes transactions]}
          (recording-driver (fn [_ params] [{"status" "acquired" "token" (:token params) "leaseUntil" 1234567}]))
          db (store/with-driver driver "neo4j" reader)]
      (-> (store/acquire-refresh! db {:name "preferences" :language "py"} 360000)
          (.then (fn [lease]
                   (is (= :acquired (:status lease)))
                   (is (string? (:token lease)))
                   (is (= 1234567 (:lease-until lease)))
                   (is (= (core/derivation-id reader "preferences" {:type :language :value "python"}) (:derivation-id lease)))
                   (let [params (:params (last @calls))]
                     (is (= reader (:readerId params)))
                     (is (= "python" (:filterValue params)))
                     (let [^js duration (:leaseMs params)] (is (= 360000 (.toNumber duration)))))
                   (is (= 1 @transactions)) (is (= 1 @closes)) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest lease-outcomes-are-closed-and-tokens-are-fenced
  (async done
    (let [cases [{:reply [{"status" "busy"}] :status :busy} {:reply [] :status :unavailable}]
          attempt (fn [{:keys [reply status]}]
                    (let [{:keys [driver]} (recording-driver (fn [_ _] reply))]
                      (-> (store/acquire-refresh! (store/with-driver driver "neo4j" reader) {:name "preferences"} 360000)
                          (.then (fn [result] (is (= status (:status result))))))))]
      (-> (js/Promise.all (to-array (map attempt cases)))
          (.then (fn [_]
                   (let [{:keys [driver calls]} (recording-driver (fn [_ _] []))
                         db (store/with-driver driver "neo4j" reader)]
                     (-> (store/renew-refresh! db {:name "preferences"} "old-token" 360000)
                         (.then (fn [result]
                                  (is (= :fenced (:status result)))
                                  (is (= "old-token" (:token (:params (last @calls)))))
                                  (store/release-refresh! db {:name "preferences"} "old-token" :timeout)))
                         (.then (fn [result]
                                  (is (= :fenced (:status result)))
                                  (is (= "old-token" (:token (:params (last @calls)))))))))))
          (.then (fn [_] (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest invalid-lease-requests-fail-before-opening-a-session
  (async done
    (let [calls (atom 0) db (store/with-driver #js {:session (fn [_] (swap! calls inc))} "neo4j" reader)
          cases [[{:name "preferences" :repository "foo/bar"} 360000]
                 [{:name "preferences"} 0] [{:name "preferences"} 86400001]
                 [{:name "preferences" :reader-id "other"} 360000]]]
      (-> (js/Promise.all
            (to-array (map (fn [[request duration]]
                             (-> (store/acquire-refresh! db request duration) (.catch config/error-code))) cases)))
          (.then (fn [codes]
                   (is (every? #{:invalid-selector :invalid-lease} (array-seq codes)))
                   (is (zero? @calls)) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest schema-readiness-and-write-fencing-recognize-node-uniqueness-types
  (async done
    (let [labels ["AdamDerivation" "AdamDerivationRun" "AdamDerivedObservation"]
          cases [["UNIQUENESS" labels true]
                 ["NODE_PROPERTY_UNIQUENESS" labels true]
                 ["NODE_PROPERTY_UNIQUENESS" (pop labels) false]
                 ["RELATIONSHIP_PROPERTY_UNIQUENESS" labels false]
                 ["NODE_PROPERTY_EXISTENCE" labels false]]
          attempt (fn [[constraint-type available ready?]]
                    (let [{:keys [driver calls closes]}
                          (recording-driver
                            (fn [query params]
                              (if (re-find #"SHOW INDEXES" query) [{"count" 1}]
                                [{"status" "acquired" "token" (:token params) "leaseUntil" 1234567}]))
                            (fn [query]
                              ;; Simulate the server's type filtering over schema-only rows.
                              (if (.includes query (str "'" constraint-type "'")) available [])))
                          db (store/with-driver driver "neo4j" reader)
                          capture (fn [work] (-> work (.then (fn [_] :ready)) (.catch config/error-code)))]
                      (-> (capture (store/check-schema! db))
                          (.then (fn [result]
                                   (testing constraint-type (is (= (if ready? :ready :schema-unavailable) result)))
                                   (capture (store/acquire-refresh! db {:name "preferences"} 60000))))
                          (.then (fn [result]
                                   (testing constraint-type (is (= (if ready? :ready :schema-unavailable) result)))
                                   (is (= 2 @closes))
                                   (when-not ready? (is (every? #(re-find #"SHOW CONSTRAINTS" (:query %)) @calls))))))))]
      (-> (js/Promise.all (to-array (map attempt cases)))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest schema-initializes-derived-identities-and-native-entry-lookup
  (async done
    (let [{:keys [driver calls closes transactions]} (recording-driver (fn [_ _] []))]
      (-> (store/ensure-schema! (store/with-driver driver "neo4j" reader))
          (.then (fn [_]
                   (is (= 4 (count @calls)))
                   (is (re-find #"adamentry_native_entry_id" (:query (last @calls))))
                   (is (= 0 @transactions))
                   (is (= 1 @closes)) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest missing-constraints-reject-concurrent-first-acquisitions-before-data-writes
  (async done
    (let [cases [[] ["AdamDerivation"] ["AdamDerivation" "AdamDerivationRun"]]
          attempt (fn [labels]
                    (let [{:keys [driver calls closes]} (recording-driver (fn [_ _] []) labels)
                          db (store/with-driver driver "neo4j" reader)]
                      (-> (js/Promise.all
                            #js [(-> (store/acquire-refresh! db {:name "preferences"} 360000) (.catch config/error-code))
                                 (-> (store/acquire-refresh! db {:name "preferences"} 360000) (.catch config/error-code))])
                          (.then (fn [codes]
                                   (is (= [:schema-unavailable :schema-unavailable] (vec (array-seq codes))))
                                   (is (= 2 (count @calls)))
                                   (is (= 2 @closes)))))))]
      (-> (js/Promise.all (to-array (map attempt cases)))
          (.then (fn [_] (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))
