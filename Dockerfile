# Build both binaries once and copy them into a distroless runtime image.
FROM golang:1.24-bookworm AS build

WORKDIR /src

# Dependencies are copied first so that a change to the source does not
# invalidate the module download layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO is off so the binaries are static and run on the distroless base.
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api && \
    go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker && \
    go build -trimpath -ldflags="-s -w" -o /out/chaos ./cmd/chaos && \
    go build -trimpath -ldflags="-s -w" -o /out/loadgen ./cmd/loadgen

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/api /usr/local/bin/api
COPY --from=build /out/worker /usr/local/bin/worker
COPY --from=build /out/chaos /usr/local/bin/chaos
COPY --from=build /out/loadgen /usr/local/bin/loadgen

USER nonroot:nonroot
EXPOSE 8080

# CMD rather than ENTRYPOINT, so that `command:` in docker-compose (or an
# argument to `docker run`) selects a different binary instead of being
# appended as arguments to this one. With an ENTRYPOINT here, a compose
# service asking for the worker would silently run a second copy of the API.
CMD ["/usr/local/bin/api"]
