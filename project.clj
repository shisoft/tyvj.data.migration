(defproject tyvj2joyoi "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jdbc "0.7.3"]
                 [mysql/mysql-connector-java "5.1.44"]
                 [less-awful-ssl "1.0.1"]
                 [http-kit "2.2.0"]
                 [org.clojure/data.json "0.2.6"]
                 [cheshire "5.8.0"]
                 ;; https://mvnrepository.com/artifact/org.jsoup/jsoup
                 [org.jsoup/jsoup "1.11.2"]]
  :main ^:skip-aot tyvj2joyoi.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :plugins [[lein-ancient "0.6.14"]])
