(ns adam.derivation.config-test
  (:require [adam.derivation.config :as config]
            [cljs.test :refer [deftest is]]))

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
                  "derivations: {preferences: {version: 1, guidance: secret, model: {provider: fake}}}"]]
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
