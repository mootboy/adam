(ns adam.knowledge.memory-migration-test
  (:require [adam.knowledge.memory-migration :as migration]
            [adam.knowledge.store :as store]
            [cljs.test :refer [async deftest is]]))

(defrecord MemoryMigrationStore [version calls fail?]
  store/MemoryIdentityMigrationStore
  (memory-identity-version! [_ _]
    (js/Promise.resolve @version))
  (migrate-memory-identities! [_ user-id target-version]
    (swap! calls conj [user-id target-version])
    (if fail?
      (js/Promise.reject (js/Error. "memory migration interrupted"))
      (do
        (reset! version target-version)
        (js/Promise.resolve nil)))))

(deftest migrates-outdated-memory-identities
  (async done
    (let [version (atom 0)
          calls (atom [])
          migration-store (->MemoryMigrationStore version calls false)]
      (-> (migration/migrate-if-needed!
           {:store migration-store :user-id "urn:adam:user:user-1"})
          (.then
           (fn [result]
             (is (= {:status :migrated :version migration/target-version} result))
             (is (= [["urn:adam:user:user-1" migration/target-version]] @calls))
             (is (= migration/target-version @version))
             (done)))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))

(deftest skips-current-memory-identities
  (async done
    (let [calls (atom [])
          migration-store
          (->MemoryMigrationStore (atom migration/target-version) calls false)]
      (-> (migration/migrate-if-needed!
           {:store migration-store :user-id "urn:adam:user:user-1"})
          (.then
           (fn [result]
             (is (= {:status :current :version migration/target-version} result))
             (is (empty? @calls))
             (done)))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))

(deftest preserves-memory-version-after-interruption
  (async done
    (let [version (atom 0)
          migration-store (->MemoryMigrationStore version (atom []) true)]
      (-> (migration/migrate-if-needed!
           {:store migration-store :user-id "urn:adam:user:user-1"})
          (.then
           (fn [_]
             (is false "expected migration failure")
             (done)))
          (.catch
           (fn [error]
             (is (= "memory migration interrupted" (.-message error)))
             (is (= 0 @version))
             (done)))))))
