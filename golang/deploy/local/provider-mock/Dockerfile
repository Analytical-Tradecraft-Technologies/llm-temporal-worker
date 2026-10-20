# Local-only deterministic fixture. Production deployments must use a reviewed
# provider endpoint and the pinned worker image; this image is never published.
FROM python:3.15-alpine
WORKDIR /srv
RUN apk add --no-cache openssl \
    && openssl req -x509 -newkey rsa:2048 -nodes \
      -keyout /srv/key.pem -out /srv/cert.pem \
      -subj '/CN=provider-mock' -days 3650 \
    && chmod 0444 /srv/key.pem /srv/cert.pem
COPY server.py .
USER 65532:65532
EXPOSE 8081
ENTRYPOINT ["python", "/srv/server.py"]
