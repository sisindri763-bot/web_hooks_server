FROM nginx:alpine

# Remove default nginx html files
RUN rm -rf /usr/share/nginx/html/*

# Copy frontend static files
COPY index.html /usr/share/nginx/html/
COPY vithi_dashboard.html /usr/share/nginx/html/
COPY legacy_index.html /usr/share/nginx/html/

# Copy custom nginx configuration
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
