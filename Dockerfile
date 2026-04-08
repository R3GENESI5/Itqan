FROM nginx:alpine

# Remove default nginx site
RUN rm /etc/nginx/conf.d/default.conf

# Copy custom nginx config
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy site files
COPY index.html /usr/share/nginx/html/
COPY app/       /usr/share/nginx/html/app/
COPY quran/     /usr/share/nginx/html/quran/

# Ensure JSON MIME type is set (alpine nginx includes it by default,
# but we add it explicitly in case the base image changes)
RUN grep -q 'application/json' /etc/nginx/mime.types || \
    sed -i '/types {/a\    application/json  json;' /etc/nginx/mime.types

EXPOSE 80
