# from pyspark.sql import SparkSession
# # Create a SparkSession
# spark = SparkSession.builder \
#     .appName("Simple PySpark Example") \
#     .getOrCreate()

# # Create a simple DataFrame
# data = [("Alice", 1), ("Bob", 2), ("Catherine", 3)]
# df = spark.createDataFrame(data, ["Name", "Age"])

# # Show the DataFrame
# df.show()


from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Cassandra connection parameters
hostname = 'localhost'  # or 'cassandra' if you're connecting from another Docker container
#That means that if we deployed this application on another docker container
#madam les containers yemcho m3a ba3dahom then we use the hostname ta3 container cassandra_db
#li houwa cassandra
port = 9042
username = 'cassandra'
password = 'cassandra'

# Authenticator using username and password
auth_provider = PlainTextAuthProvider(
    username=username, password=password)

# Establish connection to Cassandra
cluster = Cluster([hostname], port=port, auth_provider=auth_provider)
session = cluster.connect()

# Execute a sample query
rows = session.execute("SELECT cluster_name, data_center FROM system.local")
for row in rows:
    print(f"Cluster: {row.cluster_name}, Data Center: {row.data_center}")

# Close the connection
cluster.shutdown()