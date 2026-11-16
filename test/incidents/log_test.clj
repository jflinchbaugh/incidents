(ns incidents.log-test
  (:require [clojure.string :as str]
            [clojure.test :as t]
            [incidents.log :as log]
            [taoensso.telemere :as tel]))

(t/deftest test-format-signal-one-line
  (let [formatter (log/format-signal-one-line-fn)]
    (t/testing "formats signal with data on one line"
      (let [sig (tel/with-signal
                  (tel/log!
                   {:level :info
                    :data {:feed-unchanged "2026-11-16T12:00:00Z"}}))
            out (formatter sig)]
        (t/is (= 1 (count (str/split-lines out))))
        (t/is (str/includes? out "data: {:feed-unchanged"))))

    (t/testing "formats signal with message and data on one line"
      (let [sig (tel/with-signal
                  (tel/log!
                   {:level :info
                    :data {:found 10}}
                   "Facts processed"))
            out (formatter sig)]
        (t/is (= 1 (count (str/split-lines out))))
        (t/is (str/includes? out "Facts processed data: {:found 10}"))))

    (t/testing "formats signal with message only on one line"
      (let [sig (tel/with-signal
                  (tel/log! :info "Starting server."))
            out (formatter sig)]
        (t/is (= 1 (count (str/split-lines out))))
        (t/is (str/includes? out "Starting server."))))

    (t/testing "formats signal with error putting data on first line"
      (let [sig (tel/with-signal
                  (tel/log!
                   {:level :error
                    :data {:msg "Scheduled task failure"}
                    :error (Exception. "sample error")}))
            out (formatter sig)
            lines (str/split-lines out)]
        (t/is (str/includes? (first lines)
                             "data: {:msg \"Scheduled task failure\"}"))
        (t/is (str/includes? out "<<< error <<<"))))))

(t/deftest test-logging-handler-output
  (log/init!)
  (t/testing "handler writes data on one line"
    (let [out (with-out-str
                (tel/log!
                 {:level :info
                  :data {:feed-unchanged "2026-11-16T12:00:00Z"}}))]
      (t/is (= 1 (count (str/split-lines out))))
      (t/is (str/includes? out "data: {:feed-unchanged")))))
