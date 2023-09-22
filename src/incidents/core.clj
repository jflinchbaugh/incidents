(ns incidents.core
  (:gen-class)
  (:require [feedparser-clj.core :as fp]
            [clojure.java.io :as io]
            [chime.core :as chime]
            [xtdb.api :as xt]
            [tick.core :as tc]
            [taoensso.timbre :as log]
            [clojure.string :as str]
            [hiccup.page :as p]
            [hiccup.element :as e]
            [hiccup.util :as u]
            [nextjournal.clerk :as clerk]))

(log/merge-config! {:ns-filter #{"incidents.*"}})

(def feed-url "https://webcad.lcwc911.us/Pages/Public/LiveIncidentsFeed.aspx")

(def xtdb-server-url "http://localhost:4321")

(defn start-xtdb!
  []
  (xt/new-api-client xtdb-server-url))

(defn tag [type rec]
  (assoc rec :type type))

(defn prune [rec]
  (into {}
        (remove
         (fn [[_ v]]
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

(defn make-feed-rec [instant rec]
  (assoc {:date (str instant)
          :unix-time (unix-time instant)}
    :doc rec))

(defn put-feed! [node feed]
    (->>
     feed
     (make-feed-rec (tc/now))
     (tag :feed)
     add-feed-id
     (keys->db "incidents.feed")
     make-put-tx
     (xt/submit-tx node)
     (xt/await-tx node)))

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
   [[::xt/put
     {:xt/id :incidents.feed/last-feed-time
      :incidents.feed/feed-time feed-time}]]
   (xt/submit-tx node)
   (xt/await-tx node)))

(defn get-last-feed-time [node]
  (or
   (->>
    (xt/q
     (xt/db node)
     '{:find [?last-feed-time]
       :where [[?e :xt/id :incidents.feed/last-feed-time]
               [?e :incidents.feed/feed-time ?last-feed-time]]})
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
               (count ended-facts)
               "for"
               (:date feed))]
        (doall
         (concat
          (->>
           ended-facts
           (map (partial end (tc/inst (:date feed))))
           (map (partial put-fact! node)))
          (->>
           updated-facts
           (map (partial put-fact! node)))
          [(put-last-feed-time! node (:unix-time feed))]))))))

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

(defn reload-js [period delay]
  [:script
   (->
     "const scheduleReload = function(period, delay) {
        const now = new Date().getTime();
        const nextMinute = Math.trunc((now + period) / period) * period;
        const waitTime = nextMinute - now + delay;
        console.log(location.href);
        setTimeout(
          function() {
            var request = new XMLHttpRequest();
            request.onreadystatechange = function() {
              if (this.readyState == this.HEADERS_RECEIVED) {
                location.reload();
              }
            };
            request.open('GET', location.href, true);
            request.responseType = 'text/plain';
            request.send();
            scheduleReload(period, delay);
          },
          waitTime);
     };
     scheduleReload(${period}, ${delay});"
     (str/replace "${period}" (str period))
     (str/replace "${delay}" (str delay))
     )]
 )

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
                      "Clerk")
                     (reload-js 10000 0)])))))

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
   (fn [xtdb-node _]
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

(defn delete-all-facts! [node]
  (let [ids (map first
              (xt/q (xt/db node) '{:find [?e]
                                   :where [[?e :incidents.fact/type :fact]]}))]
    (->>
      ids
      (mapv #(-> [::xt/delete %]))
      (xt/submit-tx node))))

(comment

  (start-clerk!)

  (with-open [node (start-xtdb!)]
    (delete-all-facts! node)
    )

  (build-clerk! "output")

  (with-open [node (start-xtdb!)]
    (put-last-feed-time! node 0))

  .)
