# PostgreSQL 15 with multi-database support
FROM postgres:15-alpine

# Add custom initialization scripts
COPY init-scripts/ /docker-entrypoint-initdb.d/

# Make scripts executable
RUN chmod +x /docker-entrypoint-initdb.d/*.sql 2>/dev/null || true

# Set default environment
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres

# Expose PostgreSQL port
EXPOSE 5432
