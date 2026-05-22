ARG CLOUD_SDK_VERSION=emulators
FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:${CLOUD_SDK_VERSION}

ENV DATASTORE_PROJECT_ID=datastore-pandas-emulator
EXPOSE 8081

HEALTHCHECK --interval=5s --timeout=3s --start-period=20s --retries=20 \
    CMD python3 -c "import socket; s=socket.create_connection(('127.0.0.1', 8081), 2); s.close()"

CMD ["sh", "-c", "gcloud emulators firestore start --database-mode=datastore-mode --host-port=0.0.0.0:8081 --project=${DATASTORE_PROJECT_ID}"]
