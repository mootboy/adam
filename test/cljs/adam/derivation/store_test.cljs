(ns adam.derivation.store-test
  (:require [adam.derivation.config :as config]
            [adam.derivation.core :as core]
            [adam.derivation.store :as store]
            [cljs.test :refer [async deftest is testing]]))

(def reader "urn:adam:user:test-reader")

(defn recording-driver [reply]
  (let [calls (atom []) closes (atom 0) transactions (atom 0)
        run (fn [query params]
              (let [params (js->clj params :keywordize-keys true)]
                (swap! calls conj {:query query :params params})
                (js/Promise.resolve
                  #js {:records (to-array
                                 (map (fn [row] #js {:get (fn [key] (get row key))})
                                      (if (re-find #"RETURN u.id AS userId" query)
                                        [{"userId" reader}] (reply query params))))})))
        tx #js {:run run}
        session #js {:run run :executeWrite (fn [f] (swap! transactions inc) (f tx))
                     :executeRead (fn [f] (swap! transactions inc) (f tx))
                     :close (fn [] (swap! closes inc) (js/Promise.resolve nil))}]
    {:driver #js {:session (fn [_] session)} :calls calls :closes closes :transactions transactions}))

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
                     (is (= 360000 (:leaseMs params))))
                   (is (= 1 @transactions)) (is (= 1 @closes)) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest lease-outcomes-are-closed-and-tokens-are-fenced
  (async done
    (let [cases [{:reply [{"status" "busy"}] :status :busy}
                 {:reply [] :status :unavailable}]
          attempt (fn [{:keys [reply status]}]
                    (let [{:keys [driver]} (recording-driver (fn [_ _] reply))]
                      (-> (store/acquire-refresh! (store/with-driver driver "neo4j" reader) {:name "preferences"} 360000)
                          (.then #(is (= status (:status %)))))))]
      (-> (js/Promise.all (to-array (map attempt cases)))
          (.then (fn [_]
                   (let [{:keys [driver calls]} (recording-driver (fn [_ _] []))
                         db (store/with-driver driver "neo4j" reader)]
                     (-> (store/renew-refresh! db {:name "preferences"} "old-token" 360000)
                         (.then (fn [result]
                                  (is (= :fenced (:status result)))
                                  (is (re-find #"refreshToken = \$token" (:query (last @calls))))
                                  (is (re-find #"refreshLeaseUntil > timestamp\(\)" (:query (last @calls))))
                                  (store/release-refresh! db {:name "preferences"} "old-token" :timeout)))
                         (.then (fn [result]
                                  (is (= :fenced (:status result)))
                                  (is (re-find #"refreshToken = \$token" (:query (last @calls))))))))))
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

(deftest schema-is-limited-to-the-three-derived-labels
  (async done
    (let [{:keys [driver calls closes]} (recording-driver (fn [_ _] []))]
      (-> (store/ensure-schema! (store/with-driver driver "neo4j" reader))
          (.then (fn [_]
                   (is (= 3 (count @calls)))
                   (is (every? #(re-find #"IS UNIQUE" (:query %)) @calls))
                   (is (= 1 @closes)) (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))
