(ns adam.derivation.selector-test
  (:require [adam.derivation.selector :as selector]
            [adam.derivation.config :as config]
            [cljs.test :refer [deftest is]]))

(deftest equivalent-origins-select-the-same-result-bucket
  (is (= {:name "preferences" :filter {:type :repository :value "github.com/example/repo"}}
         (selector/normalize-refresh {:name "preferences" :repository "git@github.com:example/repo.git"})))
  (is (= (selector/normalize-refresh {:repository "git@github.com:example/repo.git"})
         (selector/normalize-refresh {:repository "https://user:secret@github.com/example/repo.git"}))))

(deftest one-filter-only-and-source-scoped-sessions
  (doseq [request [{:repository "git@github.com:example/repo" :language "python"}
                   {:session "session-a"} {:language "not-a-language"} {:name nil}
                   {:reader-id "impersonation"} {:repository nil}]]
    (try (selector/normalize-refresh request) (is false "must reject selector")
         (catch :default error (is (= :invalid-selector (config/error-code error))))))
  (is (= {:filter {:type :none}} (selector/normalize-refresh {})))
  (is (= "clojurescript" (get-in (selector/normalize-refresh {:language "CLJS"}) [:filter :value])))
  (is (= "urn:adam:session:user-a:claude-code:session-a"
         (get-in (selector/normalize-refresh {:session "urn:adam:session:user-a:claude-code:session-a"}) [:filter :value]))))

(deftest language-filters-only-file-provenance
  (is (selector/matches-filter? {:type :language :value "python"} {:linked-paths ["src/a.py"]}))
  (is (not (selector/matches-filter? {:type :language :value "python"} {:content "Use Python"})))
  (is (selector/matches-filter? {:type :none} {:content "Use Python"})))

(deftest read-defaults-list-buckets-rather-than-selecting-only-the-unfiltered-bucket
  (let [request (selector/normalize-read {})]
    (is (nil? (:filter request)))
    (is (false? (:history? request)))
    (is (= 10 (:limit request))))
  (doseq [request [{:history "true"} {:limit 21} {:limit 0} {:user "other"}]]
    (try (selector/normalize-read request) (is false "must reject read arguments")
         (catch :default error (is (= :invalid-selector (config/error-code error)))))))
