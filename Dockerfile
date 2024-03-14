FROM gcr.io/distroless/static-debian11:nonroot
ENTRYPOINT ["/baton-snowflake"]
COPY baton-snowflake /