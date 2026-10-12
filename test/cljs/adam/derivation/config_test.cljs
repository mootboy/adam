(ns adam.derivation.config-test
  (:require [adam.derivation.config :as config]
            [cljs.test :refer [deftest is]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop :include-macros true]
            ["yaml" :as yaml]))

(deftest parses-named-guidance-with-session-model-and-agreed-defaults
  (let [parsed (config/parse-config
                "derivations:\n  preferences:\n    version: 1\n    guidance: Preserve scope and conflicting evidence.\n")
        recipe (get parsed "preferences")]
    (is (= 1 (:version recipe)))
    (is (= "Preserve scope and conflicting evidence." (:guidance recipe)))
    (is (nil? (:model recipe)))
    (is (= {:max-output-tokens 8192 :timeout-ms 300000} (:limits recipe)))))

(deftest rejects-unsafe-or-ambiguous-yaml-without-echoing-content
  (doseq [source ["derivations: {}"
                  "derivations: {preferences: {version: 1, guidance: secret, api_key: secret}}"
                  "derivations: {preferences: {version: 1, version: 2, guidance: secret}}"
                  "derivations: {preferences: {version: 1, guidance: !!js/function secret}}"
                  "derivations: {preferences: &x {version: 1, guidance: secret}, other: *x}"
                  "derivations: {preferences: {version: 1, guidance: secret, limits: {timeout_ms: 0}}}"
                  "derivations: {preferences: {version: 1, guidance: secret, limits: {timeout_ms: 2147483648}}}"
                  "derivations: {preferences: {version: 1, guidance: secret, evidence: {require_supporting_memory_ids: false}}}"
                  "derivations: {preferences: {version: 1, guidance: secret, model: {provider: fake}}}"
                  "derivations: {preferences: {version: 1, guidance: secret, scope: {distinguish: [personal, personal]}}}"
                  "derivations: {preferences: {version: 1, guidance: secret, categories: {style: {guidance: ok, extra: secret}}}}"
                  "derivations: {preferences: {version: 1, guidance: secret, conflicts: {policy: hide}}}"
                  "derivations: {preferences: {version: 1, guidance: secret, limits: {max_input_tokens: 1.5}}}"
                  "derivations: {preferences: {version: 1, guidance: secret, evidence: {inferred_min_distinct_occasions: 0}}}"]]
    (try (config/parse-config source) (is false "must reject invalid configuration")
         (catch :default error
           (is (= :invalid-config (config/error-code error)))
           (is (not (.includes (.-message error) "secret")))))))

(deftest overrides-resolve-provider-model-and-limits-without-credentials
  (let [recipe (get (config/parse-config
                    "derivations:\n  preferences:\n    version: 2\n    guidance: Preserve scope.\n    model: {provider: fake, id: model-2}\n    limits: {max_input_tokens: 100000, max_output_tokens: 16000, timeout_ms: 600000}\n") "preferences")]
    (is (= {:provider "fake" :id "model-2"} (:model recipe)))
    (is (= {:max-input-tokens 100000 :max-output-tokens 16000 :timeout-ms 600000} (:limits recipe)))
    (is (= "/tmp/config/adam/derivations.yaml" (config/config-path "/tmp/config")))))

(deftest generated-valid-configurations-round-trip-through-the-parser
  (let [result (tc/quick-check
                 80
                 (prop/for-all [version (gen/choose 1 100000) output (gen/choose 1 16000)
                                timeout (gen/choose 1 1000000) threshold (gen/choose 1 10)]
                   (let [source (.stringify yaml (clj->js {:derivations {"preferences" {:version version :guidance "Preserve scope."
                                                                                    :model {:provider "fake" :id "model"}
                                                                                    :limits {:max_output_tokens output :timeout_ms timeout}
                                                                                    :evidence {:inferred_min_distinct_occasions threshold}}}}))
                         recipe (get (config/parse-config source) "preferences")]
                     (and (= version (:version recipe)) (= output (get-in recipe [:limits :max-output-tokens]))
                          (= timeout (get-in recipe [:limits :timeout-ms]))
                          (= threshold (:inferred-min-distinct-occasions recipe))
                          (= {:provider "fake" :id "model"} (:model recipe)))))
                 :seed 20261012)]
    (is (:pass? result) (pr-str result))))
