(ns incidents.core
  (:gen-class)
  (:require [feedparser-clj.core :as feed]
            [clojure.java.io :as io]
            [chime.core :as chime]
            [xtdb.api :as xt]
            [xtdb.remote-api-client :as xtc]
            [clojure.pprint :as pp]
            [tick.core :as tc]
            [taoensso.timbre :as log]
            [clojure.string :as str]
            [hiccup.core :as h]
            [hiccup.page :as p]
            [hiccup.element :as e]
            [hiccup.util :as u]
            [nextjournal.clerk :as clerk]
            [clojure.test :as t]))

(log/merge-config! {:ns-filter #{"incidents.*"}})

(def xtdb-server-url "http://localhost:4321")

(defn start-xtdb!
  []
  (xt/new-api-client xtdb-server-url))

(defn tag [type rec]
  (assoc rec :type type))

(defn unwrap-description
  [rec]
  (assoc
   rec
   :description (select-keys
                 (:description rec)
                 (keys (:description rec)))))

(defn add-stage-id [stage]
  (assoc stage :xt/id (tag :stage {:uri (:uri stage)})))

(defn prune [rec]
  (into {}
        (remove
         (fn [[k v]]
           (or
            (nil? v)
            (and
             (or
              (string? v)
              (coll? v))
             (empty? v)))) rec)))

(defn keys->db [namespace mem-record]
  (update-keys
   mem-record
   (fn [k]
     (if (#{:xt/id} k) k
         (keyword (str namespace "/" (name k)))))))

(defn keys->mem [db-record]
  (update-keys
   db-record
   (fn [k]
     (if (#{:xt/id} k) k
         (keyword (name k))))))

(defn put-stage! [node stage]
  (let [write (keys->db "incidents.stage" stage)]
    (xt/await-tx
     node
     (xt/submit-tx
      node
      [[::xt/put write]]))))

(defn incident-type [fact]
  (cond
    (nil? fact) nil
    (nil? (:title fact)) nil
    (re-matches #".*(Standby).*" (:title fact)) :fire
    (re-matches #".*(EMS|Medical|Transfer).*" (:title fact)) :medical
    (re-matches #".*(Traffic|Vehicle).*" (:title fact)) :traffic
    :else :fire))

(defn get-all-stage [node]
  (->>
   (xt/q (xt/db node) '{:find [(pull e [*])]
                        :where [[e :incidents.stage/type :stage]]})
   (mapv first)
   (mapv keys->mem)))

(defn get-all-stage-ids [node]
  (->>
   (xt/q
    (xt/db node)
    '{:find [e]
      :where [[e :incidents.stage/type :stage]]})
   (map first)))

(defn clear-all-stage! [node]
  (->>
   (get-all-stage-ids node)
   (mapv (fn [i] [::xt/evict i]))
   (xt/submit-tx node)
   (xt/await-tx node)))

(defn delete-stage! [node ids]
  (->>
   ids
   (mapv (fn [i] [::xt/delete i]))
   (xt/submit-tx node)
   (xt/await-tx node)))

(defn load-stage! [node source]
  (let [new-stage (->>
                   source
                   feed/parse-feed
                   :entries
                   (map prune)
                   (map unwrap-description)
                   (map (partial tag :stage))
                   (map add-stage-id)
                   (sort-by :uri))
        new-stage-ids (set (map :xt/id new-stage))
        existing-stage (sort-by :uri (get-all-stage node))
        existing-stage-ids (map :xt/id existing-stage)
        ids-to-remove (remove new-stage-ids existing-stage-ids)
        removals (delete-stage! node ids-to-remove)
        updates (remove (set existing-stage) new-stage)
        _ (log/info
           (str
            "Found "
            (count new-stage)
            ", Updated "
            (count updates)
            ", Removed "
            (count ids-to-remove)))]
    (->>
     updates
     (map (partial put-stage! node))
     doall)))

(defn override [s]
  (get
   {"Bls" "BLS"
    "Ems" "EMS"
    "Qrs" "QRS"
    "Tac" "TAC"
    "Int" "INT"
    "1a" "1A"
    "2a" "2A"
    "3a" "3A"
    "Co" "CO"}
   s
   s))

(defn mc-fix [s]
  (str/replace
   s
   #"^Mc(.)"
   (fn [[_ letter]] (str "Mc" (str/upper-case letter)))))

(defn title-case [s]
  (str/join (map (comp mc-fix override str/capitalize) (str/split s #"\b"))))

(def format-unit (comp title-case str/trim))

(def format-street (comp title-case str/trim))

(defn parse-units [s]
  (if (nil? s) '()
      (map format-unit (str/split s #"<br>"))))

(defn parse-streets [s]
  (if (nil? s) '()
      (map format-street (str/split s #"[&/]"))))

(defn format-title [title]
  (if (nil? title) nil
      (->
       title
       (str/replace #" +- +" "-")
       title-case
       str/trim)))

(defn format-municipality [name]
  (if (nil? name) nil
      (str/trim (title-case name))))

(defn add-incident-type [fact]
  (if (nil? fact) nil
      (assoc fact :incident-type (incident-type fact))))

(defn parse [in]
  (let [parts (str/split (get-in in [:description :value]) #"; *")
        [municipality streets units] parts
        [streets units] (if
                         (re-matches #".*COUNTY$" municipality)
                          [nil streets]
                          [streets units])]
    (->>
     {:uri (:uri in)
      :start-date (:published-date in)
      :active? true
      :title (format-title (:title in))
      :municipality (format-municipality municipality)
      :streets (parse-streets streets)
      :units (parse-units units)}
     add-incident-type)))

(defn add-fact-id [fact]
  (assoc fact :xt/id (tag :fact {:uri (:uri fact)})))

(defn put-fact! [node fact]
  (let [write (keys->db "incidents.fact" fact)]
    (xt/await-tx
     node
     (xt/submit-tx
      node
      [[::xt/put write]]))))

(defn get-all-active-facts [node]
  (->>
   (xt/q (xt/db node) '{:find [(pull ?e [*])]
                        :where [[?e :incidents.fact/type :fact]
                                [?e :incidents.fact/start-date]
                                [?e :incidents.fact/active? true]]})
   (mapv first)
   (mapv keys->mem)))

(defn get-all-facts [node]
  (->>
   (xt/q (xt/db node) '{:find [(pull ?e [*])]
                        :where [[?e :incidents.fact/type :fact]
                                [?e :incidents.fact/start-date]]})
   (mapv first)
   (mapv keys->mem)))

(defn end [date fact]
  (assoc fact
         :end-date date
         :active? false
         :duration-minutes (tc/minutes (tc/between (:start-date fact) date))))

(defn transform-facts! [node]
  (let [active-facts (get-all-active-facts node)
        new-facts (->>
                   node
                   get-all-stage
                   (map parse)
                   (map (partial tag :fact))
                   (map add-fact-id))
        updated-facts (remove (set active-facts) new-facts)
        ended-facts (remove (set new-facts) active-facts)]

    (doall
     (concat
      (->>
       ended-facts
       (map (partial end (tc/inst)))
       (map (partial put-fact! node)))
      (->>
       updated-facts
       (map (partial put-fact! node)))))))

(defn format-date-part [fmt d]
  (tc/format fmt (tc/date-time d)))

(defn format-date-time [d]
  (format-date-part "yyyy-MM-dd HH:mm:ss" d))

(defn format-date [d]
  (format-date-part "yyyy-MM-dd" d))

(defn format-streets [streets]
  (str/join " & " (or streets [])))

(defn format-map-location
  [municipality streets]
  (str/join
   " "
   [(format-streets streets)
    municipality
    "PA"]))

(defn map-link
  ([municipality streets]
   (u/url "https://www.google.com/maps/search/"
          {:api 1 :query (format-map-location municipality streets)}))
  ([fact]
   (map-link (:municipality fact) (:streets fact))))

(defn report-entry [f]
  [:li
   [:div.streets
    (e/link-to
     {:target "_blank"}
     (map-link f)
     (format-streets (:streets f)))]
   [:div.municipality (:municipality f)]
   [:div.start-date (format-date-time (:start-date f))]
   [:div.title (:title f)]
   [:div.units (str/join ", " (:units f))]])

(defn format-incident-type [type]
  (str/capitalize (name type)))

(defn report-active [facts output-dir]
  (let [title "Active Incidents"
        out-file (io/file output-dir "index.html")]
    (io/make-parents out-file)
    (spit out-file
          (str
           (p/html5 {:lang "en"}
                    [:head
                     [:title title]
                     (p/include-css "style.css")]
                    [:body
                     [:h1 title]
                     (apply
                      concat
                      (for [type [:traffic :fire :medical]]
                        (let [incidents (->>
                                         facts
                                         (filter
                                          (fn [fact]
                                            (= type (:incident-type fact))))
                                         (sort-by :start-date)
                                         reverse)]
                          [[:h2
                            (format-incident-type type)
                            " ("
                            (count incidents)
                            ")"]
                           [:ul
                            (map
                             report-entry
                             incidents)]])))
                     (e/link-to
                      {:target "_blank"}
                      "clerk/index.html"
                      "Clerk")])))))

(defn copy-file! [src dest]
  (io/copy (io/input-stream (io/resource src)) (io/file dest)))

(defn copy-resources! [dest]
  (copy-file! "web/htaccess" (str dest "/.htaccess"))
  (copy-file! "web/style.css" (str dest "/style.css")))

(defn start-clerk! []
  (clerk/serve! {:browse? true :watch-paths ["notebooks"]}))

(defn stop-clerk! []
  (clerk/halt!))

(defn build-clerk! [out-path]
  (clerk/build! {:paths ["notebooks/incidents.clj"]
                 :out-path (io/file out-path "clerk")}))

(def disconnected-actions
  {"clerk" (fn [args]
             (let [[output-dir] args]
               (if (str/blank? output-dir)
                 (log/info "clerk <output-dir>")
                 (build-clerk! output-dir))))})

(def connected-actions
  {"load"
   (fn [xtdb-node args]
     (doall
      (concat
       (load-stage!
        xtdb-node
        "https://webcad.lcwc911.us/Pages/Public/LiveIncidentsFeed.aspx")
       (transform-facts! xtdb-node))))

   "list-active"
   (fn [xtdb-node args]
     (let [stage (get-all-stage xtdb-node)
           facts (get-all-active-facts xtdb-node)]
       (->> stage (map pp/pprint) doall)
       (log/info "Count of Stage:" (count stage))
       (->> facts (map pp/pprint) doall)
       (log/info "Count of Facts:" (count facts))))

   "list-all"
   (fn [xtdb-node args]
     (let [stage (get-all-stage xtdb-node)
           facts (get-all-facts xtdb-node)]
       (doall (map prn stage))
       (log/info "Count of Stage:" (count stage))
       (doall (map prn facts))
       (log/info "Count of Facts:" (count facts))))

   "clear"
   (fn [xtdb-node args]
     (doall (map #(log/info %) (clear-all-stage! xtdb-node))))})

(defn load-and-report [xtdb-node [output-dir]]
  (doall
   (concat
    (load-stage!
     xtdb-node
     "https://webcad.lcwc911.us/Pages/Public/LiveIncidentsFeed.aspx")
    (transform-facts! xtdb-node)))
  (let [facts (get-all-active-facts xtdb-node)]
    (report-active facts output-dir)
    (copy-resources! output-dir)))

(defn wait-forever []
  (loop [] (Thread/sleep java.lang.Integer/MAX_VALUE) (recur)))

(defn server [xtdb-node [seconds output-dir]]
  (let [load-schedule (chime/chime-at
                       (chime/periodic-seq
                         (tc/now)
                         (tc/of-seconds (parse-long seconds)))
                       (fn [time]
                         (load-and-report xtdb-node [output-dir])))
        clerk-schedule (chime/chime-at
                        (rest
                          (chime/periodic-seq
                            (tc/now)
                            (tc/of-seconds (* 4 (parse-long seconds)))))
                        (fn [time]
                          (build-clerk! output-dir)))]
    (wait-forever)))

(def connected-report-actions
  {"report-active"
   (fn [xtdb-node args]
     (let [facts (get-all-active-facts xtdb-node)
           output-dir (first args)]
       (report-active facts output-dir)
       (copy-resources! output-dir)))

   "load-and-report"
   load-and-report

   "server"
   server})

(defn -main [& args]
  (let [[action & args] args
        disconnected-action (get disconnected-actions action)
        connected-action (get connected-actions action)
        connected-report-action (get connected-report-actions action)]
    (cond
      disconnected-action
      (disconnected-action args)

      connected-action
      (with-open [xtdb-node (start-xtdb!)]
        (connected-action xtdb-node args))

      connected-report-action
      (if (str/blank? (first args))
        (log/info "Usage:" action "<output-dir>")
        (with-open [xtdb-node (start-xtdb!)]
          (connected-report-action xtdb-node args)))

      :else
      (log/info
       "Usage:"
       (->>
        [connected-actions connected-report-actions disconnected-actions]
        (map keys)
        (apply concat)
        (str/join "|")))))
  (shutdown-agents))

(comment
  (start-clerk!)

  (stop-clerk!)

  (build-clerk! "output")

  (with-open [xtdb-node (start-xtdb!)]
    (transform-facts! xtdb-node))

  (with-open [xtdb-node (start-xtdb!)]
    (get-all-active-facts xtdb-node))

  (with-open [xtdb-node (start-xtdb!)]
    (get-all-facts xtdb-node))

  (with-open [xtdb-node (start-xtdb!)]
    (load-stage!
     xtdb-node
     "https://webcad.lcwc911.us/Pages/Public/LiveIncidentsFeed.aspx"))

  (-main "load")

  (-main "list-active")

  (-main "list-all")

  (-main "clear")

  (-main)

  (start-xtdb!)

  (def xtdb-node (xt/new-api-client xtdb-server-url))

  (with-open [node (start-xtdb!)]
    (->>
     (xt/attribute-stats node)))

  (with-open [node (start-xtdb!)]
    (->> node
         get-all-facts
         (map :title)
         (group-by identity)
         keys
         sort))

  (with-open [node (start-xtdb!)]
    (->> node
         get-all-facts
         (mapv (partial keys->db "incidents.fact"))
         (mapv keys->mem)
         (take 10)))

  (def scheduler
    (chime/chime-at
     (chime/periodic-seq (tc/now) (tc/of-seconds 5))
     (fn [time] (prn time))))

  (.close scheduler)

  (def load-scheduler
    (chime/chime-at
     (chime/periodic-seq (tc/now) (tc/of-seconds 10))
     (fn [time] (-main "load-and-report" "output"))))

  (.close load-scheduler)

  (with-open [node (start-xtdb!)]
    (server node [10 "output"]))

  (-main "server" "10" "output")

  (with-open [xtdb-node (start-xtdb!)]
    (let [output-dir "output"
          time (tc/now)]
      (do
        (load-and-report xtdb-node [output-dir])
        (build-clerk! output-dir)
        (prn time))))

  .)
