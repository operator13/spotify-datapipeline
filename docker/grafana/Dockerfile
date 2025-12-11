# =============================================================================
# Grafana Dockerfile
# Custom Grafana image for Spotify Data Quality Dashboard
# =============================================================================

FROM grafana/grafana:10.2.0

# Switch to root for setup
USER root

# Create directories for provisioning (use numeric UID 472 for grafana user)
RUN mkdir -p /var/lib/grafana/dashboards && \
    chown -R 472:472 /var/lib/grafana

# Copy provisioning configuration
COPY provisioning/datasources/ /etc/grafana/provisioning/datasources/
COPY provisioning/dashboards/dashboard.yml /etc/grafana/provisioning/dashboards/

# Copy actual dashboard JSON files to the dashboards directory
COPY provisioning/dashboards/data-quality-overview.json /var/lib/grafana/dashboards/

# Set permissions (use numeric UID 472 for grafana user)
RUN chown -R 472:472 /etc/grafana/provisioning && \
    chown -R 472:472 /var/lib/grafana/dashboards

# Switch back to grafana user
USER 472

# Environment variables
ENV GF_SECURITY_ADMIN_USER=admin
ENV GF_SECURITY_ADMIN_PASSWORD=admin
ENV GF_USERS_ALLOW_SIGN_UP=false

# Expose Grafana port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl --fail http://localhost:3000/api/health || exit 1
