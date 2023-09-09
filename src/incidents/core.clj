(ns incidents.core
  (:gen-class)
  (:require [feedparser-clj.core :as fp]
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

(def feed-url "https://webcad.lcwc911.us/Pages/Public/LiveIncidentsFeed.aspx")

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

(defn make-put-tx [data]
  [[::xt/put data]])

(defn add-feed-id [feed]
  (assoc feed :xt/id (tag :feed {:date (:date feed)})))

(defn unix-time [inst]
  (tc/millis (tc/between (tc/epoch) inst)))

(defn put-feed! [node feed]
  (let [now-instant (tc/now)]
    (->>
     feed
     (assoc {:date (str now-instant)
             :unix-time (unix-time now-instant)}
            :doc)
     (tag :feed)
     (add-feed-id)
     (keys->db "incidents.feed")
     make-put-tx
     (xt/submit-tx node)
     (xt/await-tx node))))

(defn load-feed! [node source]
  (->>
   source
   slurp
   (put-feed! node)))

(defn get-feeds-since [node last-time]
  (->>
   (xt/q
    (xt/db node)
    '{:find [(pull ?e [*]) ?unix-time]
      :where [[?e :incidents.feed/type :feed]
              [?e :incidents.feed/unix-time ?unix-time]
              [(< ?last-time ?unix-time)]]
      :order-by [[?unix-time :asc]]
      :in [?last-time]}
    last-time)
   (mapv first)
   (mapv keys->mem)))

(defn incident-type [fact]
  (cond
    (nil? fact) nil
    (nil? (:title fact)) nil
    (re-matches #".*(Standby).*" (:title fact)) :fire
    (re-matches #".*(EMS|Medical|Transfer).*" (:title fact)) :medical
    (re-matches #".*(Traffic|Vehicle).*" (:title fact)) :traffic
    :else :fire))

(defn put-last-feed-time! [node feed-time]
  (->>
   [[::xt/put {:xt/id :last-feed-time :feed-time feed-time}]]
   (xt/submit-tx node)
   (xt/await-tx node)))

(defn get-last-feed-time [node]
  (or
   (->>
    (xt/q
     (xt/db node)
     '{:find [?last-feed-time]
       :where [[?e :xt/id :last-feed-time]
               [?e :feed-time ?last-feed-time]]})
    ffirst)
   0))

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
      (make-put-tx write)))))

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

(defn transform-facts!
  [node]
  (let [last-feed-time (get-last-feed-time node)
        feeds (get-feeds-since node last-feed-time)]
    (for [feed feeds]
      (let [new-facts (->>
                       feed
                       :doc
                       java.io.StringBufferInputStream.
                       fp/parse-feed
                       :entries
                       (map prune)
                       (map unwrap-description)
                       (sort-by :uri)
                       (map parse)
                       (map (partial tag :fact))
                       (map add-fact-id))
            active-facts (get-all-active-facts node)
            updated-facts (remove (set active-facts) new-facts)
            ended-facts (remove (set new-facts) active-facts)
            _ (log/info
               "Found"
               (count active-facts)
               "Updating"
               (count updated-facts)
               "Ending"
               (count ended-facts))]
        (doall
         (concat
          (->>
           ended-facts
              ;; TODO use the feed date
           (map (partial end (tc/inst)))
           (map (partial put-fact! node)))
          (->>
           updated-facts
           (map (partial put-fact! node)))
          [(put-last-feed-time! node (:unix-time feed))]))))))

(comment
  (with-open [node (start-xtdb!)]
    (transform-facts! node)))

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

(defn log-lines [s]
  (doseq [line (str/split s #"\n")]
    (log/info line)))

(defn build-clerk! [out-path]
  (->
   {:paths ["notebooks/incidents.clj"]
    :out-path (io/file out-path "clerk")}
   clerk/build!
   with-out-str
   log-lines))

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
       (load-feed! xtdb-node feed-url)
       (transform-facts! xtdb-node))))})

(defn load-and-report [xtdb-node [output-dir]]
  (doall
   (concat
    (load-feed! xtdb-node feed-url)
    (transform-facts! xtdb-node)))
  (let [facts (get-all-active-facts xtdb-node)]
    (report-active facts output-dir)
    (copy-resources! output-dir)))

(defn wait-forever []
  (loop [] (Thread/sleep java.lang.Integer/MAX_VALUE) (recur)))

(defn server [xtdb-node [ingest-seconds clerk-seconds output-dir]]
  (log/info "Starting server.")
  (let [load-schedule (chime/chime-at
                       (chime/periodic-seq
                        (tc/now)
                        (tc/of-seconds (parse-long ingest-seconds)))
                       (fn [time]
                         (load-and-report xtdb-node [output-dir])))
        clerk-schedule (chime/chime-at
                        (chime/periodic-seq
                         (tc/now)
                         (tc/of-seconds (parse-long clerk-seconds)))
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
    (server node [10 60 "output"]))

  (tc/instant (tc/millis (tc/between (tc/epoch) (tc/now))))

  (with-open [node (start-xtdb!)]
    (load-feed! node feed-url))

  (with-open [node (start-xtdb!)]
    (->>
     (get-feed-since node 0)
     (map
      (fn [feed]
        (->>
         feed
         :doc
         java.io.StringBufferInputStream.
         fp/parse-feed
         :entries
         (map prune)
         (map unwrap-description)
         (sort-by :uri)
         (map parse)
         (map (partial tag :fact))
         (map add-fact-id))))))

  .)
