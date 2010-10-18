(defproject store "0.0.1-SNAPSHOT"
  :description "Distributed datastorage wrapping s3 and Voldemort"
  :dependencies [[org.clojure/clojure "1.2.0-master-SNAPSHOT"]
                 [org.clojure/clojure-contrib "1.2.0-SNAPSHOT"]
                 [net.java.dev.jets3t/jets3t "0.7.4"]
		 [clomert "0.2.0"]
		 [clj-serializer "0.1.0"]]
  :dev-dependencies [[swank-clojure "1.3.0-SNAPSHOT"]
		     [lein-clojars "0.5.0"]])