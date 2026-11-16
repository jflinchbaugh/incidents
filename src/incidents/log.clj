(ns incidents.log
  (:require [clojure.string :as str]
            [taoensso.telemere :as tel]
            [taoensso.telemere.utils :as tel-utils]
            [taoensso.trove :as trove]
            [taoensso.trove.telemere :as trove-telemere]))

(defn format-signal-one-line-fn
  ([]
   (format-signal-one-line-fn nil))
  ([opts]
   (let [preamble-fn (or (:preamble-fn opts)
                         (tel-utils/signal-preamble-fn opts))
         content-fn (or (:content-fn opts)
                        (tel-utils/signal-content-fn opts))]
     (fn [signal]
       (let [preamble (when preamble-fn (preamble-fn signal))
             content (when content-fn (content-fn signal))]
         (if content
           (if-let [err-idx (str/index-of content "<<< error <<<")]
             (let [before (subs content 0 err-idx)
                   after (subs content err-idx)
                   single (->> (str/split-lines before)
                               (map str/trim)
                               (remove str/blank?)
                               (str/join " "))]
               (if (str/blank? single)
                 (str preamble "\n" after "\n")
                 (str (str/join " " (remove str/blank? [preamble single]))
                      "\n"
                      after
                      "\n")))
             (let [single (->> (str/split-lines content)
                               (map str/trim)
                               (remove str/blank?)
                               (str/join " "))]
               (str (str/join " " (remove str/blank? [preamble single]))
                    "\n")))
           (str (or preamble "") "\n")))))))

(defn init!
  "Initializes application logging:
  - Sets Trove to forward logs to Telemere (for Datahike)
  - Sets namespace filter to incidents.*
  - Configures :default/console handler with format-signal-one-line-fn"
  ([]
   (init! {:ns-filter #{"incidents.*"}}))
  ([{:keys [ns-filter]}]
   (trove/set-log-fn! (trove-telemere/get-log-fn))
   (when ns-filter
     (tel/set-ns-filter! ns-filter))
   (tel/add-handler!
    :default/console
    (tel/handler:console
     {:output-fn (format-signal-one-line-fn)})
    {:async nil})))
