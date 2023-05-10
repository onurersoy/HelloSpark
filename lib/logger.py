# 4.0: We created this Python package and class to create a new Class for handling log4J and expose simple and
# -easy to use methods to create a log entry.

class Log4J:
    def __init__(self, spark):
        # 4.1: Get the log4j instance from the spark session's java virtual machine (JVM):
        log4j = spark._jvm.org.apache.log4j

        root_class = "guru.learningjournal.spark.examples"
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")

        self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)

    # 4.2: Let's define some easy to use methods:
    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def message(self, message):
        self.logger.message(message)