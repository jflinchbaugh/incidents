(ns incidents.core
  (:gen-class)
  (:require [feedparser-clj.core :as fp]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [chime.core :as chime]
            [datahike.api :as d]
            [tick.core :as tc]
            [taoensso.timbre :as log]
            [clojure.string :as str]
            [hiccup.page :as p]
            [hiccup.element :as e]
            [hiccup.util :as u]
            [nextjournal.clerk :as clerk]
            [taoensso.trove :as trove]
            [taoensso.trove.timbre :as trove-timbre]))

(trove/set-log-fn! (trove-timbre/get-log-fn))
(log/merge-config! {:ns-filter #{"incidents.*"}})

(def feed-url "https://webcad.lcwc911.us/Pages/Public/LiveIncidentsFeed.aspx")

(def schema
  [{:db/ident :incidents.feed/id
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :incidents.feed/date
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :incidents.feed/unix-time
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :incidents.feed/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :incidents.fact/uri
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :incidents.fact/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :incidents.fact/streets
    :db/valueType :db.type/tuple
    :db/cardinality :db.cardinality/one}
   {:db/ident :incidents.fact/units
    :db/valueType :db.type/tuple
    :db/cardinality :db.cardinality/one}])

(def db-config
  {:store {:backend :file
           :path "data"
           :id #uuid "7c9e6679-7425-40de-944b-e07fc1f90ae7"}
   :schema-flexibility :read
   :keep-history? false
   :commit-graph? false
   :online-gc {:enabled? true
               :grace-period-ms 0}
   :max-string-length 0
   :initial-tx schema})

(defn start-db!
  ([]
   (start-db! db-config))
  ([config]
   (when-not (d/database-exists? config)
     (d/create-database config))
   (d/connect config)))

(def db-lock (Object.))

(defmacro with-db
  [[sym & args] & body]
  `(locking db-lock
     (let [~sym (start-db! ~@args)]
       (try
         ~@body
         (finally
           (d/release ~sym))))))

(defn gc! [node]
  (when (not= :memory (get-in (d/db node) [:config :store :backend]))
    @(d/gc-storage node (java.util.Date.) {:min-age-ms 0})))

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
     (keyword (str namespace "/" (name k))))))

(defn keys->mem [db-record]
  (update-keys
   (dissoc (into {} db-record) :db/id)
   (fn [k]
     (keyword (name k)))))

(defn make-put-tx [data]
  [data])

(defn unix-time [inst]
  (tc/millis (tc/between (tc/epoch) inst)))

(defn make-feed-rec [instant rec]
  (assoc {:date (str instant)
          :unix-time (unix-time instant)}
    :doc rec))

(defn make-last-feed-time-tx [feed-time]
  (make-put-tx
   {:incidents.feed/id :incidents.feed/last-feed-time
    :incidents.feed/feed-time feed-time}))

(defn put-last-feed-time! [node feed-time]
  (->>
   (make-last-feed-time-tx feed-time)
   (d/transact node)))

(defn get-last-feed-time [node]
  (or
   (->>
    (d/q
     '{:find [?last-feed-time]
       :where [[?e :incidents.feed/id :incidents.feed/last-feed-time]
               [?e :incidents.feed/feed-time ?last-feed-time]]}
     (d/db node))
    ffirst)
   0))

(defn get-last-feed [node]
  (let [db (d/db node)]
    (if-let [feed-date (:incidents.feed/feed-date
                        (d/entity db [:incidents.feed/id
                                      :incidents.feed/last-feed]))]
      (some-> (d/entity db [:incidents.feed/date feed-date]) keys->mem)
      (let [last-time (get-last-feed-time node)]
        (some->>
         (d/q
          '{:find [?e ?unix-time]
            :where [[?e :incidents.feed/type :feed]
                    [?e :incidents.feed/unix-time ?unix-time]
                    [(<= ?last-time ?unix-time)]]
            :order-by [?unix-time :desc]
            :limit 1
            :in [$ ?last-time]}
          db
          last-time)
         ffirst
         (d/entity db)
         keys->mem)))))

(defn make-feeds-tx [feeds]
  (let [db-feeds (mapv (partial keys->db "incidents.feed") feeds)]
    (conj db-feeds
          {:incidents.feed/id :incidents.feed/last-feed
           :incidents.feed/feed-date
           (:incidents.feed/date (peek db-feeds))})))

(defn make-feed-tx [feed]
  (make-feeds-tx [feed]))

(defn put-feed!
  ([node feed]
   (put-feed! node (tc/now) feed))
  ([node instant feed]
   (if (= feed (:doc (get-last-feed node)))
     (log/info (str "Feed unchanged, saving nothing for " instant))
     (->>
      feed
      (make-feed-rec instant)
      (tag :feed)
      make-feed-tx
      (d/transact node)))))

(defn load-feed! [node source]
  (->>
   source
   slurp
   (put-feed! node)))

(def feed-batch-size 100)

(def gc-batch-interval 10)

(defn get-feed-ids-since [db last-time]
  (->>
   (d/q
    '{:find [?e ?unix-time]
      :where [[?e :incidents.feed/type :feed]
              [?e :incidents.feed/unix-time ?unix-time]
              [(< ?last-time ?unix-time)]]
      :order-by [?unix-time :asc]
      :in [$ ?last-time]}
    db
    last-time)
   (mapv first)))

(defn get-feeds-batch [db feed-ids]
  (->>
   (d/q
    '{:find [(pull ?e [*]) ?unix-time]
      :where [[?e :incidents.feed/unix-time ?unix-time]]
      :order-by [?unix-time :asc]
      :in [$ [?e ...]]}
    db
    feed-ids)
   (mapv first)
   (mapv keys->mem)))

(defn get-feeds-since [node last-time]
  (let [db (d/db node)]
    (->>
     (get-feed-ids-since db last-time)
     (partition-all feed-batch-size)
     (mapcat (partial get-feeds-batch db)))))

(defn incident-type [fact]
  (cond
    (nil? fact) nil
    (nil? (:title fact)) nil
    (re-matches #".*(Standby).*" (:title fact)) :fire
    (re-matches #".*(EMS|Medical|Transfer).*" (:title fact)) :medical
    (re-matches #".*(Traffic|Vehicle).*" (:title fact)) :traffic
    :else :fire))

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
    "Co" "CO"
    "Ecc" "ECC"
    "Ema" "EMA"
    "Micu" "MICU"}
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
  (if (nil? s) []
      (mapv format-unit (str/split s #"<br>"))))

(defn parse-streets [s]
  (if (nil? s) []
      (mapv format-street (str/split s #"[&/]"))))

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

(defn put-fact! [node fact]
  (->> fact
       (keys->db "incidents.fact")
       make-put-tx
       (d/transact node)))

(defn get-all-active-facts [node]
  (->>
   (d/q '{:find [(pull ?e [*])]
          :where [[?e :incidents.fact/type :fact]
                  [?e :incidents.fact/start-date]
                  [?e :incidents.fact/active? true]]}
        (d/db node))
   (mapv first)
   (mapv keys->mem)))

(defn get-all-facts [node]
  (->>
   (d/q '{:find [(pull ?e [*])]
          :where [[?e :incidents.fact/type :fact]
                  [?e :incidents.fact/start-date]]}
        (d/db node))
   (mapv first)
   (mapv keys->mem)))

(defn end [date fact]
  (assoc fact
         :end-date date
         :active? false
         :duration-minutes (tc/minutes (tc/between (:start-date fact) date))))

(defn make-fact-tx [fact]
  (make-put-tx (keys->db "incidents.fact" fact)))

(defn parse-feed [feed]
  (->>
   feed
   :doc
   java.io.StringBufferInputStream.
   fp/parse-feed
   :entries
   (map prune)
   (sort-by :uri)
   (map parse)
   (mapv (partial tag :fact))))

(defn feed-fact-txs [active-facts feed]
  (let [new-facts (or (:facts feed) (parse-feed feed))
        new-uris (set (map :uri new-facts))
        updated-facts (remove (set active-facts) new-facts)
        ended-facts (remove (comp new-uris :uri) active-facts)]
    (log/info
     "Found"
     (count active-facts)
     "Updating"
     (count updated-facts)
     "Ending"
     (count ended-facts)
     "for"
     (:date feed))
    {:active-facts new-facts
     :tx-ops (vec
              (concat
               (->>
                ended-facts
                (map (partial end (tc/inst (:date feed))))
                (mapcat make-fact-tx))
               (mapcat make-fact-tx updated-facts)))}))

(defn transform-feeds-batch! [node active-facts feeds]
  (let [feeds (pmap (fn [feed] (assoc feed :facts (parse-feed feed))) feeds)
        {:keys [active-facts tx-ops]}
        (reduce
         (fn [state feed]
           (let [{:keys [active-facts tx-ops]}
                 (feed-fact-txs (:active-facts state) feed)]
             {:active-facts active-facts
              :tx-ops (reduce
                       (fn [acc op]
                         (update acc (:incidents.fact/uri op) merge op))
                       (:tx-ops state)
                       tx-ops)}))
         {:active-facts active-facts
          :tx-ops {}}
         feeds)]
    {:active-facts active-facts
     :txs (if-let [last-time (:unix-time (last feeds))]
            [(d/transact
              node
              (into (vec (vals tx-ops))
                    (make-last-feed-time-tx last-time)))]
            [])}))

(defn transform-facts!
  [node]
  (let [db (d/db node)
        last-feed-time (get-last-feed-time node)
        feed-id-batches (->>
                         (get-feed-ids-since db last-feed-time)
                         (partition-all feed-batch-size))]
    (when (seq feed-id-batches)
      (:txs
       (reduce
        (fn [{:keys [active-facts txs batch-num]} feed-ids]
          (let [feeds (get-feeds-batch db feed-ids)
                batch-res (transform-feeds-batch!
                           node
                           active-facts
                           feeds)
                next-batch-num (inc batch-num)]
            (when (zero? (mod next-batch-num gc-batch-interval))
              (gc! node))
            {:active-facts (:active-facts batch-res)
             :txs (into txs (:txs batch-res))
             :batch-num next-batch-num}))
        {:active-facts (get-all-active-facts node)
         :txs []
         :batch-num 0}
        feed-id-batches)))))

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
  [:li.incident-card {:class (name (:incident-type f))}
   [:div.streets
    (e/link-to
     {:target "_blank"}
     (map-link f)
     (format-streets (:streets f)))]
   [:div.municipality
    (e/link-to
      {:target "_blank"}
      (map-link f)
      (:municipality f))]
   [:div.start-date (format-date-time (:start-date f))]
   [:div.title (:title f)]
   [:div.units (str/join ", " (:units f))]])

(defn format-incident-type [type]
  (str/capitalize (name type)))

(defn reload-js [period delay]
   [:script
    (->
      "scheduleReload(${period}, ${delay});"
      (str/replace "${period}" (str period))
      (str/replace "${delay}" (str delay)))])

(defn report-active [facts output-dir]
  (let [title "Active Incidents"
        out-file (io/file output-dir "index.html")]
    (io/make-parents out-file)
    (spit out-file
          (str
           (p/html5 {:lang "en"}
                    [:head
                     [:meta {:name "viewport"
                             :content "width=device-width, initial-scale=1"}]
                     [:title title]
                     (p/include-css "style.css")
                     [:link
                      {:rel "shortcut icon"
                       :href "emergency-light.png"}]]
                    [:body
                     [:header.main-header
                      [:img.logo {:src "emergency-light.png" :alt ""}]
                      [:h1 title]]
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
                     [:footer
                      (e/link-to
                       {:target "_blank"}
                       "clerk/index.html"
                       "Clerk")
                      " • "
                      (e/link-to
                       {:target "_blank"}
                       "https://www.lcwc911.us/live-incident-list"
                       "Lancaster Live Incidents")]
                     (p/include-js "reloader.js")
                     (reload-js 60000 0)])))))

(defn copy-file! [src dest]
  (io/copy (io/input-stream (io/resource src)) (io/file dest)))

(defn copy-resources! [dest]
  (copy-file! "web/htaccess" (str dest "/.htaccess"))
  (copy-file! "web/emergency-light.png" (str dest "/emergency-light.png"))
  (copy-file! "web/style.css" (str dest "/style.css"))
  (copy-file! "web/reloader.js" (str dest "/reloader.js")))

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

(defn evict-all! [node attr val]
  (loop [txs []]
    (let [ids (mapv first
                    (d/q {:find '[?e]
                          :where [['?e attr val]]
                          :limit feed-batch-size}
                         (d/db node)))]
      (if (seq ids)
        (let [_ (log/info "Retracting" (count ids) "for" attr val)
              tx (->>
                  ids
                  (mapv (fn [id] [:db/retractEntity id]))
                  (d/transact node))]
          (recur (conj txs tx)))
        (do
          (gc! node)
          txs)))))

(defn delete-all-facts! [node]
  (evict-all! node :incidents.fact/type :fact))

(defn reset-facts! [node]
  (delete-all-facts! node)
  (put-last-feed-time! node 0))

(defn export-feeds! [node file]
  (with-open [out (-> file
                      io/output-stream
                      java.util.zip.GZIPOutputStream.
                      io/writer)]
    (binding [*out* out]
      (doseq [feed (get-feeds-since node 0)]
        (prn (select-keys feed [:date :doc]))))))

(defn changed-feeds [last-doc entries]
  (lazy-seq
   (when-let [[{:keys [date doc]} & more] (seq entries)]
     (let [instant (tc/instant date)]
       (if (= doc last-doc)
         (do
           (log/info (str "Feed unchanged, saving nothing for " instant))
           (changed-feeds last-doc more))
         (cons (tag :feed (make-feed-rec instant doc))
               (changed-feeds doc more)))))))

(defn import-feeds! [node file]
  (with-open [in (-> file
                     io/input-stream
                     java.util.zip.GZIPInputStream.
                     io/reader)]
    (let [entries (pmap edn/read-string (line-seq in))
          last-doc (:doc (get-last-feed node))
          batches (->> entries
                       (changed-feeds last-doc)
                       (partition-all feed-batch-size)
                       (pmap (fn [batch]
                               [batch (make-feeds-tx batch)])))]
      (doseq [[idx [batch tx-data]] (map-indexed vector batches)]
        (log/info "Importing" (count batch) "for" (:date (first batch)))
        (d/transact node tx-data)
        (when (zero? (mod (inc idx) gc-batch-interval))
          (gc! node)))
      (gc! node))))

(defn delete-all-feeds! [node]
  (d/transact
   node
   [[:db/retractEntity [:incidents.feed/id :incidents.feed/last-feed]]])
  (evict-all! node :incidents.feed/type :feed))

(def connected-actions
  {"load"
   (fn [node _]
     (doall
      (concat
       (load-feed! node feed-url)
       (transform-facts! node))))

   "reset"
   (fn [node _]
     (reset-facts! node))

   "purge-feeds"
   (fn [node _]
     (delete-all-feeds! node))

   "gc"
   (fn [node _]
     (gc! node))

   "export"
   (fn [node [file]]
     (if (str/blank? file)
       (log/info "export <file>")
       (export-feeds! node file)))

   "import"
   (fn [node [file]]
     (if (str/blank? file)
       (log/info "import <file>")
       (import-feeds! node file)))})

(defn load-and-report [node [output-dir]]
  (doall
   (concat
    (load-feed! node feed-url)
    (transform-facts! node)))
  (let [facts (get-all-active-facts node)]
    (report-active facts output-dir)
    (copy-resources! output-dir)))

(defn wait-forever []
  (loop [] (Thread/sleep java.lang.Integer/MAX_VALUE) (recur)))

(defn server [[ingest-seconds clerk-seconds output-dir]]
  (log/info "Starting server.")
  (let [load-schedule (chime/chime-at
                       (chime/periodic-seq
                        (tc/now)
                        (tc/of-seconds (parse-long ingest-seconds)))
                       (fn [time]
                         (with-db [node]
                           (load-and-report node [output-dir]))))
        clerk-schedule (chime/chime-at
                        (chime/periodic-seq
                         (tc/now)
                         (tc/of-seconds (parse-long clerk-seconds)))
                        (fn [time]
                          (build-clerk! output-dir)))]
    (wait-forever)))

(def connected-report-actions
  {"report-active"
   (fn [node args]
     (let [facts (get-all-active-facts node)
           output-dir (first args)]
       (report-active facts output-dir)
       (copy-resources! output-dir)))

   "load-and-report"
   load-and-report})

(def disconnected-actions
  {"clerk"
   (fn [args]
     (let [[output-dir] args]
       (if (str/blank? output-dir)
         (log/info "clerk <output-dir>")
         (build-clerk! output-dir))))

   "server"
   (fn [args]
     (if (str/blank? (first args))
       (log/info "server <ingest-seconds> <clerk-seconds> <output-dir>")
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
      (with-db [node]
        (connected-action node args))

      connected-report-action
      (if (str/blank? (first args))
        (log/info "Usage:" action "<output-dir>")
        (with-db [node]
          (connected-report-action node args)))

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

  (with-db [node]
    (delete-all-facts! node)
    )

  (build-clerk! "output")

  (with-db [node]
    (put-last-feed-time! node 0))

  .)
