(ns adam.derivation.evidence-test
  (:require [adam.derivation.evidence :as evidence]
            [adam.derivation.config :as config]
            [cljs.test :refer [deftest is]]))

(def own "urn:adam:user:owner")
(def other "urn:adam:user:other")
(def session "urn:adam:session:owner:pi:session-a")
(defn observation [id owner entry]
  {:id id :owner-id owner :session-id session :kind :observation
   :producer "test-producer" :content "A source-grounded preference"
   :source-occasions [{:session-id session :stream-id "main" :entry-id entry}]})

(deftest expands-reflection-support-without-counting-repeated-summaries-as-occasions
  (let [a (observation "a" own "user-1")
        b (observation "b" own "user-1")
        c (observation "c" own "user-2")
        reflection {:id "r" :owner-id own :session-id session :kind :reflection
                    :producer "test-producer" :content "Synthesized preference"
                    :supporting-memory-ids ["a" "b" "c"]}
        snapshot (evidence/select-input {:reader-id own :filter {:type :none}
                                         :memories [a b c reflection] :grants []})]
    (is (= 2 (count (evidence/support-occasions snapshot ["r" "a" "b"]))))))

(deftest direct-grants-do-not-reverse-or-propagate
  (let [grants [{:owner-id other :reader-id own :permission "read"}
                {:owner-id "third" :reader-id other :permission "read"}]
        memories [(observation "a" own "one") (observation "b" other "two")
                  (observation "c" "third" "three")]
        select #(evidence/select-input {:reader-id % :memories memories :grants grants :filter {:type :none}})]
    (is (= ["a" "b"] (:candidate-ids (select own))))
    (is (= ["b" "c"] (:candidate-ids (select other))))))

(deftest dropped-support-is-retained-but-not-independently-promoted
  (let [dropped (assoc (observation "a" own "one") :dropped? true)
        reflection {:id "r" :owner-id own :session-id session :kind :reflection
                    :producer "test-producer" :content "Preference" :supporting-memory-ids ["a"]}
        snapshot (evidence/select-input {:reader-id own :memories [dropped reflection]})]
    (is (= ["r"] (:candidate-ids snapshot)))
    (is (= #{:historical-support} (get-in snapshot [:memories 0 :roles])))
    (is (= 1 (count (evidence/support-occasions snapshot ["r"]))))))

(deftest comparison-inherits-all-original-dependencies-without-new-evidence-strength
  (let [a (observation "a" own "one")
        b (assoc (observation "b" other "two") :dropped? true)
        comparison {:id "run-old" :input-memory-ids ["b"] :conclusions [{:id "old" :content "Private finding"}]}
        snapshot (evidence/select-input {:reader-id own :memories [a b]
                                         :grants [{:owner-id other :reader-id own :permission "read"}]
                                         :comparison comparison})
        revoked (evidence/select-input {:reader-id own :memories [a b] :comparison comparison})]
    (is (= ["a"] (:candidate-ids snapshot)))
    (is (= #{:comparison-dependency} (get-in snapshot [:memories 1 :roles])))
    (is (= 1 (count (evidence/support-occasions snapshot ["a"]))))
    (is (nil? (:comparison revoked)))
    (is (= [:comparison-unavailable] (:diagnostics revoked)))))

(deftest comparison-cannot-claim-content-with-an-empty-dependency-list
  (let [snapshot (evidence/select-input {:reader-id own :memories []
                                         :comparison {:id "old" :input-memory-ids []
                                                      :conclusions [{:id "c" :content "Private"}]}})]
    (is (nil? (:comparison snapshot)))
    (is (= [:comparison-unavailable] (:diagnostics snapshot)))))

(deftest original-activity-anchors-survive-fork-recording-context
  (let [a (assoc (observation "a" own "one") :session-id "urn:adam:session:owner:pi:fork")
        b (assoc (observation "b" own "two") :session-id "urn:adam:session:owner:pi:fork")
        snapshot (evidence/select-input {:reader-id own :memories [a b]})]
    (is (= 2 (count (evidence/support-occasions snapshot ["a" "b"]))))))

(deftest copying-the-same-host-event-across-ownership-namespaces-is-not-repetition
  (let [a (observation "a" own "one")
        copied-session "urn:adam:session:other:pi:session-a"
        b (assoc (observation "b" other "one") :session-id copied-session
                 :source-occasions [{:session-id copied-session :stream-id "main" :entry-id "one"}])
        snapshot (evidence/select-input {:reader-id own :memories [a b]
                                         :grants [{:owner-id other :reader-id own :permission "read"}]})]
    (is (= 1 (count (evidence/support-occasions snapshot ["a" "b"]))))))

(deftest missing-reflection-support-fails-closed
  (try (evidence/select-input {:reader-id own :memories [{:id "r" :owner-id own :session-id session
                                                         :kind :reflection :producer "test"
                                                         :content "Missing source" :supporting-memory-ids ["missing"]}]})
       (is false "must reject missing dependency")
       (catch :default error (is (= :unresolved-evidence (config/error-code error))))))
