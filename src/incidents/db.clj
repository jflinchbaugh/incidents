(ns incidents.db
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [datahike.api :as d]
            [datahike-lmdb.core]
            [incidents.parse :as parse]
            [taoensso.telemere :as tel]
            [taoensso.trove :as trove]
            [taoensso.trove.telemere :as trove-telemere]
            [tick.core :as tc]))

(trove/set-log-fn! (trove-telemere/get-log-fn))
(tel/set-ns-filter! #{"incidents.*"})

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
   {:db/ident :incidents.feed/doc
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :incidents.feed/feed-time
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :incidents.feed/feed-date
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :incidents.fact/uri
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :incidents.fact/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :incidents.fact/start-date
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :incidents.fact/end-date
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :incidents.fact/active?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :incidents.fact/title
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :incidents.fact/municipality
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :incidents.fact/incident-type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :incidents.fact/duration-minutes
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :incidents.fact/streets
    :db/valueType :db.type/tuple
    :db/cardinality :db.cardinality/one}
   {:db/ident :incidents.fact/units
    :db/valueType :db.type/tuple
    :db/cardinality :db.cardinality/one}])

(def db-config
  {:store {:backend :lmdb
           :path "data"
           :id #uuid "7c9e6679-7425-40de-944b-e07fc1f90ae7"
           :map-size (* 10 1024 1024 1024)}
   :schema-flexibility :write
   :attribute-refs? true
   :keep-history? false
   :commit-graph? false
   :fuse-index-roots? true
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

(def db-conn (atom nil))

(defmacro with-shared-db
  [[& args] & body]
  `(let [conn# (locking db-lock
                 (let [c# (start-db! ~@args)]
                   (reset! db-conn c#)
                   c#))]
     (try
       ~@body
       (finally
         (locking db-lock
           (reset! db-conn nil)
           (d/release conn#))))))

(defmacro with-db
  [[sym & args] & body]
  `(locking db-lock
     (if-let [conn# ~(when (empty? args) `@db-conn)]
       (let [~sym conn#]
         ~@body)
       (let [~sym (start-db! ~@args)]
         ~(when (empty? args) `(reset! db-conn ~sym))
         (try
           ~@body
           (finally
             ~(when (empty? args) `(reset! db-conn nil))
             (d/release ~sym)))))))

(defn gc! [node]
  (when (not= :memory (get-in (d/db node) [:config :store :backend]))
    @(d/gc-storage
      node
      (java.util.Date.)
      {:min-age-ms (get-in db-config [:online-gc :grace-period-ms])})))

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
     (tel/log!
      {:level :info
       :data {:feed-unchanged instant}})
     (->>
      feed
      (make-feed-rec instant)
      (parse/tag :feed)
      make-feed-tx
      (d/transact node)))))

(defn fetch-feed
  ([source]
   (fetch-feed source 10000 30000))
  ([source conn-timeout read-timeout]
   (try
     (let [content (slurp source
                          :conn-timeout conn-timeout
                          :read-timeout read-timeout)]
       (if (str/blank? content)
         (do
           (tel/log!
            {:level :warn
             :data {:msg "Feed response was empty or blank"
                    :source (str source)}})
           nil)
         content))
     (catch Throwable t
       (tel/log!
        {:level :error
         :data {:msg "Failed to fetch feed" :source (str source)}
         :error t})
       nil))))

(defn load-feed! [node source]
  (when-let [content (fetch-feed source)]
    (put-feed! node content)))

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

(defn make-fact-tx [fact]
  (make-put-tx (keys->db "incidents.fact" fact)))

(defn feed-fact-txs [active-facts feed]
  (let [new-facts (or (:facts feed) (parse/parse-feed feed))
        new-uris (set (map :uri new-facts))
        updated-facts (remove (set active-facts) new-facts)
        ended-facts (remove (comp new-uris :uri) active-facts)]
    (tel/log!
     {:level :info
      :data {:found (count active-facts)
             :updating (count updated-facts)
             :ending (count ended-facts)
             :date (:date feed)}})
    {:active-facts new-facts
     :tx-ops (vec
              (concat
               (->>
                ended-facts
                (map (partial parse/end (tc/inst (:date feed))))
                (mapcat make-fact-tx))
               (mapcat make-fact-tx updated-facts)))}))

(defn transform-feeds-batch! [node active-facts feeds]
  (let [feeds (pmap
               (fn [feed] (assoc feed :facts (parse/parse-feed feed)))
               feeds)
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
            #_(when (zero? (mod next-batch-num gc-batch-interval))
              (gc! node))
            {:active-facts (:active-facts batch-res)
             :txs (into txs (:txs batch-res))
             :batch-num next-batch-num}))
        {:active-facts (get-all-active-facts node)
         :txs []
         :batch-num 0}
        feed-id-batches)))))

(defn evict-all! [node attr val]
  (loop [txs []]
    (let [ids (mapv first
                    (d/q {:find '[?e]
                          :where [['?e attr val]]
                          :limit feed-batch-size}
                         (d/db node)))]
      (if (seq ids)
        (let [_ (tel/log!
                 {:level :info
                  :data {:retracting (count ids)
                         :attr attr
                         :val val}})
              tx (->>
                  ids
                  (mapv (fn [id] [:db/retractEntity id]))
                  (d/transact node))]
          (recur (conj txs tx)))
        (do
          #_(gc! node)
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
           (tel/log!
            {:level :info
             :data {:feed-unchanged instant}})
           (changed-feeds last-doc more))
         (cons (parse/tag :feed (make-feed-rec instant doc))
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
        (tel/log!
         {:level :info
          :data {:importing (count batch)
                 :date (:date (first batch))}})
        (d/transact node tx-data)
        #_(when (zero? (mod (inc idx) gc-batch-interval))
          (gc! node)))
      #_(gc! node))))

(defn delete-all-feeds! [node]
  (d/transact
   node
   [[:db/retractEntity [:incidents.feed/id :incidents.feed/last-feed]]])
  (evict-all! node :incidents.feed/type :feed))
