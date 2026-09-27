(ns adam.replica.migration-test
  (:require [adam.replica.migration :as migration]
            [adam.replica.store :as store]
            [cljs.test :refer [async deftest is]]))

(defrecord MigrationStore [version calls fail?]
  store/SessionIdentityMigrationStore
  (session-identity-version! [_ _]
    (js/Promise.resolve @version))
  (migrate-pi-session-identities! [_ user-id target-version]
    (swap! calls conj [user-id target-version])
    (if fail?
      (js/Promise.reject (js/Error. "migration interrupted"))
      (do
        (reset! version target-version)
        (js/Promise.resolve nil)))))

(deftest migrates-an-outdated-user-and-advances-only-after-success
  (async done
    (let [version (atom 0)
          calls (atom [])
          migration-store (->MigrationStore version calls false)]
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

(deftest skips-a-current-user
  (async done
    (let [calls (atom [])
          migration-store (->MigrationStore (atom migration/target-version) calls false)]
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

(deftest preserves-the-old-version-after-interruption
  (async done
    (let [version (atom 0)
          migration-store (->MigrationStore version (atom []) true)]
      (-> (migration/migrate-if-needed!
           {:store migration-store :user-id "urn:adam:user:user-1"})
          (.then
           (fn [_]
             (is false "expected migration failure")
             (done)))
          (.catch
           (fn [error]
             (is (= "migration interrupted" (.-message error)))
             (is (= 0 @version))
             (done)))))))
