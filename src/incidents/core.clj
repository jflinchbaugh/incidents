(ns incidents.core
  (:gen-class)
  (:require [chime.core :as chime]
            [clojure.string :as str]
            [incidents.db :as db]
            [incidents.log :as log]
            [incidents.report :as report]
            [taoensso.telemere :as tel]
            [tick.core :as tc]))

(log/init!)

(def feed-url "https://webcad.lcwc911.us/Pages/Public/LiveIncidentsFeed.aspx")

(def connected-actions
  {"load"
   (fn [node _]
     (doall
      (concat
       (db/load-feed! node feed-url)
       (db/transform-facts! node))))

   "reset"
   (fn [node _]
     (db/reset-facts! node))

   "purge-feeds"
   (fn [node _]
     (db/delete-all-feeds! node))

   "gc"
   (fn [node _]
     (db/gc! node)
     (db/vacuum!))

   "export"
   (fn [node [file]]
     (if (str/blank? file)
       (tel/log! :info "export <file>")
       (db/export-feeds! node file)))

   "import"
   (fn [node [file]]
     (if (str/blank? file)
       (tel/log! :info "import <file>")
       (db/import-feeds! node file)))})

(defn load-and-report [node [output-dir]]
  (doall
   (concat
    (db/load-feed! node feed-url)
    (db/transform-facts! node)))
  (let [facts (db/get-all-active-facts node)]
    (report/report-active facts output-dir)
    (report/copy-resources! output-dir)))

(defn wait-forever []
  (loop [] (Thread/sleep java.lang.Integer/MAX_VALUE) (recur)))

(defn server [[ingest-seconds clerk-seconds output-dir]]
  (tel/log! :info "Starting server.")
  (let [error-handler (fn [e]
                        (tel/log!
                         {:level :error
                          :data {:msg "Scheduled task failure"}
                          :error e})
                        true)
        load-schedule (chime/chime-at
                       (chime/periodic-seq
                        (tc/now)
                        (tc/of-seconds (parse-long ingest-seconds)))
                       (fn [time]
                         (db/with-db [node]
                           (load-and-report node [output-dir])))
                       {:error-handler error-handler})
        clerk-schedule (chime/chime-at
                        (chime/periodic-seq
                         (tc/now)
                         (tc/of-seconds (parse-long clerk-seconds)))
                        (fn [time]
                          (report/build-clerk! output-dir))
                        {:error-handler error-handler})]
    (wait-forever)))

(def connected-report-actions
  {"report-active"
   (fn [node args]
     (let [facts (db/get-all-active-facts node)
           output-dir (first args)]
       (report/report-active facts output-dir)
       (report/copy-resources! output-dir)))

   "load-and-report"
   load-and-report})

(def disconnected-actions
  {"clerk"
   (fn [args]
     (let [[output-dir] args]
       (if (str/blank? output-dir)
         (tel/log! :info "clerk <output-dir>")
         (report/build-clerk! output-dir))))

   "server"
   (fn [args]
     (if (str/blank? (first args))
       (tel/log! :info "server <ingest-seconds> <clerk-seconds> <output-dir>")
       (server args)))})

(defn -main [& args]
  (let [[action & args] args
        disconnected-action (get disconnected-actions action)
        connected-action (get connected-actions action)
        connected-report-action (get connected-report-actions action)]
    (cond
      disconnected-action
      (disconnected-action args)

      connected-action
      (db/with-db [node]
        (connected-action node args))

      connected-report-action
      (if (str/blank? (first args))
        (tel/log! :info ["Usage:" action "<output-dir>"])
        (db/with-db [node]
          (connected-report-action node args)))

      :else
      (tel/log!
       :info
       ["Usage:"
        (->>
         [connected-actions connected-report-actions disconnected-actions]
         (map keys)
         (apply concat)
         (str/join "|"))])))
  (shutdown-agents))

(comment

  (report/start-clerk!)

  (db/with-db [node]
    (db/delete-all-facts! node)
    )

  (report/build-clerk! "output")

  (db/with-db [node]
    (db/put-last-feed-time! node 0))

  .)
