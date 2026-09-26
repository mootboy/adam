(ns adam.knowledge.migration-test
  (:require [adam.knowledge.migration :as migration]
            [adam.knowledge.store :as store]
            [cljs.test :refer [async deftest is]]))

(defrecord MigrationStore [version calls]
  store/CodeMemoryMigrationStore
  (code-memory-version! [_ user-id]
    (swap! calls conj [:version user-id])
    (js/Promise.resolve @version))
  (complete-code-memory-rebuild! [_ user-id target-version]
    (swap! calls conj [:complete user-id target-version])
    (reset! version target-version)
    (js/Promise.resolve nil)))

(deftest rebuilds-every-session-before-advancing-the-version-marker
  (async done
    (let [version (atom 2)
          calls (atom [])
          memory-store (->MigrationStore version calls)]
      (-> (migration/rebuild-if-needed!
           {:store memory-store
            :user-id "urn:adam:user:user-1"
            :session-files ["/sessions/b.jsonl" "/sessions/a.jsonl" "/sessions/a.jsonl"]
            :rebuild-session! (fn [path]
                                (swap! calls conj [:rebuild path])
                                (js/Promise.resolve nil))})
          (.then
           (fn [result]
             (is (= {:status :rebuilt :version 3 :session-count 2} result))
             (is (= [[:version "urn:adam:user:user-1"]
                     [:rebuild "/sessions/a.jsonl"]
                     [:rebuild "/sessions/b.jsonl"]
                     [:complete "urn:adam:user:user-1" 3]]
                    @calls))
             (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest interrupted-rebuild-does-not-advance-the-version-marker
  (async done
    (let [version (atom 2)
          calls (atom [])
          memory-store (->MigrationStore version calls)]
      (-> (migration/rebuild-if-needed!
           {:store memory-store
            :user-id "urn:adam:user:user-1"
            :session-files ["/sessions/a.jsonl" "/sessions/b.jsonl"]
            :rebuild-session! (fn [path]
                                (swap! calls conj [:rebuild path])
                                (if (.endsWith path "b.jsonl")
                                  (js/Promise.reject (js/Error. "interrupted"))
                                  (js/Promise.resolve nil)))})
          (.then (fn [_] (is false "expected rebuild failure") (done)))
          (.catch
           (fn [error]
             (is (= "interrupted" (.-message error)))
             (is (= 2 @version))
             (is (not-any? #(= :complete (first %)) @calls))
             (done)))))))

(deftest current-version-skips-session-work
  (async done
    (let [calls (atom [])
          rebuilt (atom 0)
          memory-store (->MigrationStore (atom migration/target-version) calls)]
      (-> (migration/rebuild-if-needed!
           {:store memory-store
            :user-id "urn:adam:user:user-1"
            :session-files ["/sessions/a.jsonl"]
            :rebuild-session! (fn [_]
                                (swap! rebuilt inc)
                                (js/Promise.resolve nil))})
          (.then
           (fn [result]
             (is (= {:status :current :version 3} result))
             (is (zero? @rebuilt))
             (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))
