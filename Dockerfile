# Use an official MySQL image from Docker Hub
FROM mysql:latest

# Set environment variables
# MYSQL_ROOT_PASSWORD is the root password
# MYSQL_DATABASE is the name of the database to create
# MYSQL_USER and MYSQL_PASSWORD are for an additional user
ENV MYSQL_ROOT_PASSWORD=24!dsa3101?10
ENV MYSQL_DATABASE=ga-sample
ENV MYSQL_USER=dsa3101
ENV MYSQL_PASSWORD=24!dsa3101?10

# Expose the default MySQL port
EXPOSE 3306

# Add custom SQL files if you have any to initialize the database
# COPY ./path/to/your_init_script.sql /docker-entrypoint-initdb.d/
